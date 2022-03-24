import configparser
import getpass

import atscale
import pyarrow
from atscale.errors import UserError
import atscale.db
from feast.errors import FeastProviderLoginError
import contextlib
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import (
    Callable,
    ContextManager,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

import contextlib
import os
from datetime import datetime
from pathlib import Path
from typing import (
    Callable,
    ContextManager,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

import numpy as np
import pandas as pd
import pyarrow as pa
from pydantic import Field, StrictStr
from pydantic.typing import Literal
from pytz import utc

from feast import OnDemandFeatureView
from feast.data_source import DataSource
from feast.errors import InvalidEntityType
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.snowflake_source import (
    SavedDatasetSnowflakeStorage,
    SnowflakeSource,
)
from feast.infra.utils.snowflake_utils import (
    execute_snowflake_statement,
    get_snowflake_conn,
    write_pandas,
)
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.usage import log_exceptions_and_usage

from sdk.python.feast.infra.offline_stores.atscale_source import AtScaleSource

try:
    from atscale.atscale import AtScale
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("atscale", str(e))



class AtScaleOfflineStore(OfflineStore):

    @staticmethod
    @log_exceptions_and_usage(offline_store="atscale")
    def pull_all_from_table_or_query(
            config: RepoConfig,
            data_source: DataSource,
            join_key_columns: List[str],
            feature_name_columns: List[str],
            event_timestamp_column: str,
            start_date: datetime,
            end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(data_source, AtScaleSource)

        atproject = _get_atscale_api(config)
        features = join_key_columns + feature_name_columns + [event_timestamp_column]
        atscale_query = atproject.generate_atscale_query(features=features)
        atscale_query += f"""
         WHERE {event_timestamp_column} BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')"""
        #todo: look into using get_data filter_between
        return AtScaleRetrievalJob(
            atproject=atproject,
            query=atscale_query,
            config=config,
            full_feature_names=False,
        )

    @staticmethod
    @log_exceptions_and_usage(offline_store='atscale')
    def get_historical_features(config: RepoConfig, feature_views: List[FeatureView],
                                feature_refs: List[str],
                                entity_df: Union[pd.DataFrame, str],
                                registry: Registry, project: str,
                                full_feature_names: bool = False) -> RetrievalJob:
        logging.INFO("Getting historical features from my atscale offline store")

        assert isinstance(config.offline_store, AtScaleOfflineStoreConfig)

        atproject: AtScale = _get_atscale_api(config)

        entity_schema: dict[str, np.dtype] = _get_entity_schema(entity_df, atproject, config)
        #dict from column to datatype

        entity_df_event_timestamp_col = offline_utils.infer_event_timestamp_from_entity_df(
            entity_schema
        ) #takes datetime dtype as timestamp col

        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df, entity_df_event_timestamp_col, atproject,
        )

        #todo try to do this without creating entity df in database
        #or not, don't reinvent the wheel ppl will still appreciate you ;)
        @contextlib.contextmanager
        def query_generator() -> Iterator[str]:

            #random name for new table
            table_name = offline_utils.get_temp_entity_table_name()

            _upload_entity_df(entity_df=entity_df, table_name=table_name, atproject=atproject)

            expected_join_keys = offline_utils.get_expected_join_keys(
                project, feature_views, registry
            )

            offline_utils.assert_expected_columns_in_entity_df(
                entity_schema, expected_join_keys, entity_df_event_timestamp_col
            )

            # Build a query context containing all information required to template the SQL query
            query_context = offline_utils.get_feature_view_query_context(
                feature_refs,
                feature_views,
                registry,
                project,
                entity_df_event_timestamp_range,
            )

            # Generate the Snowflake SQL query from the query context
            query = offline_utils.build_point_in_time_query(
                query_context,
                left_table_query_string=table_name, #new generated table
                entity_df_event_timestamp_col=entity_df_event_timestamp_col,
                entity_df_columns=entity_schema.keys(),
                query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
                full_feature_names=full_feature_names,
            )

            yield query

        return AtScaleRetrievalJob(
            query=query_generator,
            atproject=atproject,
            config=config,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(entity_schema.keys() - {entity_df_event_timestamp_col}),
                min_event_timestamp=entity_df_event_timestamp_range[0],
                max_event_timestamp=entity_df_event_timestamp_range[1],
            ),
        )



    def pull_latest_from_table_or_query(self,
                                        config: RepoConfig,
                                        data_source: AtScaleSource,
                                        join_key_columns: List[str],
                                        feature_name_columns: List[str],
                                        event_timestamp_column: str,
                                        created_timestamp_column: Optional[str],
                                        start_date: datetime,
                                        end_date: datetime) -> RetrievalJob:
        """
        Returns rows for the latest values based on timestamp_column for times between the start and end parameters
        returns one row for each join key in join_key_coluumns otherwise just one row
        :param RepoConfig config: the config for this offline store which will be passed to the retrieval job
        :param AtScaleSource data_source: UNUSED until we implement multi project/model support, gives the model to
        query from
        :param list join_key_columns: Entity keys that this result may be joined by, if
        join_key_columns is not empty, a row for the latest values for each join_key in the list will be returned
        :param list[str] feature_name_columns: the values to retrieve
        :param str event_timestamp_column: the time column used to determine the latest value using the same reference
         name as the source
        :param str created_timestamp_column: an optional parameter
        :param datetime start_date: earliest date to return if its the latest value
        :param datetime end_date: latest time the latest data can be from
        :rtype: AtScaleRetrievalJob
        """
        assert isinstance(data_source, AtScaleSource)
        assert isinstance(config.offline_store, AtScaleOfflineStoreConfig)

        atproject = _get_atscale_api(config.offline_store)

        feature_name_columns = [f'`{atproject.model_name}`.`{f}`' for f in feature_name_columns]
        if join_key_columns:
            partition_by_join_key_string = f'PARTITION BY ' + \
                                          ', '.join(f'`{atproject.model_name}`.`{key}`' for key in join_key_columns)
        else:
            partition_by_join_key_string = ''

        timestamp_columns = [f'`{atproject.model_name}`.`{event_timestamp_column}`']
        if created_timestamp_column:
            timestamp_columns.append(f'`{atproject.model_name}`.`{created_timestamp_column}`')

        timestamp_desc_string = " DESC, ".join(timestamp_columns) + " DESC"
        field_string = ", ".join(join_key_columns + feature_name_columns + timestamp_columns
                                 )

        start_date = start_date.astimezone(tz=utc)
        end_date = end_date.astimezone(tz=utc)

        query = f"""
                    SELECT
                        {field_string}
                        {f''', TRIM({repr(DUMMY_ENTITY_VAL)}::VARIANT,'"') AS "{DUMMY_ENTITY_ID}"''' if not join_key_columns else ""}
                    FROM (
                        SELECT {field_string},
                        ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS "_feast_row"
                        FROM `{atproject.project_name}`,`{atproject.model_name}`
                        WHERE `{atproject.model_name}`.`{event_timestamp_column}` BETWEEN TIMESTAMP '{start_date}' AND TIMESTAMP '{end_date}'
            )
                    WHERE "_feast_row" = 1
                    """

        return AtScaleRetrievalJob(
            atproject=atproject,
            query=query,
            config=config,
            full_feature_names=False,
        )

class AtScaleRetrievalJob(RetrievalJob):
    def __init__(
        self,
        atproject: AtScale,
        query: Union[str, Callable[[], ContextManager[str]]],
        config: RepoConfig,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,):
        if not isinstance(query, str):
            self._query_generator = query
        else:

            @contextlib.contextmanager
            def query_generator() -> Iterator[str]:
                assert isinstance(query, str)
                yield query

            self._query_generator = query_generator
        self.atproject = atproject
        self.config = config
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = (
            on_demand_feature_views if on_demand_feature_views else []
        )
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> Optional[List[OnDemandFeatureView]]:
        return self._on_demand_feature_views

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def _to_df_internal(self) -> pd.DataFrame:
        with self._query_generator() as query:
            df = self._execute_atquery(query)
            return df

    def to_df(self) -> pd.DataFrame:
        return self._to_df_internal()

    def to_sql(self) -> str:
        """
        Returns the SQL query that will be submitted to AtScale to rewrite the query and submit it the
        historical feature table.
        """
        with self._query_generator() as query:
            return query

    def _to_arrow_internal(self) -> pa.Table:
        """Returns the rows read as an Arrow table"""
        with self._query_generator() as query:
            pa_table = pyarrow.Table.from_pandas(self.to_df())
            if pa_table:
                return pa_table
            else:
                empty_result = self.atproject.custom_query(query=query)

                return pa.Table.from_pandas(
                    pd.DataFrame(columns=[md.name for md in empty_result.description])
                )

    def to_database(self, table_name: str) -> None:
        #todo
        raise Exception('this functionality requires db connection')

    def persist(self, storage: SavedDatasetStorage):
        #assert isinstance(storage, SavedDatasetAtScaleStorage)
        #todo requires object and db connection to use schema and table fields in obj
        raise Exception('db connection not figured out yet :(')

    def _execute_atquery(self, query) -> pd.DataFrame:
        """ Executes a query after its translation by the AtScale query planner"""
        return self.atproject.custom_query(query=query)

class AtScaleOfflineStoreConfig(FeastConfigBaseModel):
    """ Custom offline store config for AtScale"""

    type: Literal["atscale.offline"] = "atscale.offline"
    """ Offline store type selector"""

    server: StrictStr

    token: Optional[StrictStr]

    user: Optional[StrictStr]

    pw: Optional[StrictStr]

    organization: StrictStr

    project_id: StrictStr

    model_id: StrictStr

    design_center_server_port: StrictStr = '10500'

    engine_port: StrictStr = '10502'


    """Database configuration options
    (at least three aren't optional tho u rly need it, stay strapped ;)"""

    connection_id: StrictStr
    """ i.e. atscale_connection_id/data source db type"""

    username: Optional[StrictStr]
    """Redshift, iris, Snowflake, Synapse"""

    password: Optional[StrictStr]
    """Redshift, iris, Snowflake, Synapse"""

    host: Optional[StrictStr]
    """Redshift, iris, Databricks, Host"""

    driver: Optional[StrictStr]
    """Iris, Databricks, Synapse"""

    port: Optional[StrictStr]
    """Redshift, Iris, Databricks, Synapse"""

    database: Optional[StrictStr]
    """Redshift, Snowflake, Synapse"""

    schema: Optional[StrictStr]
    """Redshift, Iris, Databricks, Snowflake, Synapse"""


    """bigquery parameters"""
    credentials_path: Optional[StrictStr]

    project: Optional[StrictStr]

    dataset: Optional[StrictStr]

    """snowflake"""
    account: Optional[StrictStr]

    warehouse: Optional[StrictStr]

    """databricks"""
    token: Optional[StrictStr]

    http_path: Optional[StrictStr]

    """iris"""
    namespace: Optional[StrictStr]


def _get_entity_schema(
        entity_df: Union[pd.DataFrame, str],
        atscale_connection: AtScale
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, str):
        entity_df_sample = (
            atscale_connection.custom_query(f"SELECT * FROM ({entity_df}) LIMIT 1")
        )
        entity_schema = dict(zip(entity_df_sample.columns, entity_df_sample.dtypes))
    elif isinstance(entity_df, pd.DataFrame):
        entity_schema = dict(zip(entity_df.columns, entity_df.dypes))
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_schema

def _upload_entity_df(
        entity_df: Union[pd.DataFrame, str],
        atproject: AtScale,
        table_name: str,
) -> None:
    if isinstance(entity_df, str):
       atproject.database.submit_query(f'CREATE TABLE "{table_name}" AS ({entity_df})')
        #todo: test if you can execute create statements with submit_query
        #todo: make sure this is temporary
    elif isinstance(entity_df, pd.DataFrame):
        atproject.database.add_table(table_name=table_name, dataframe=entity_df)
    else:
        raise InvalidEntityType(type(entity_df))

def _get_atscale_api(config) -> AtScale:
    #todo, atscale.check_single_connection possible downfall and figure out if it can be the same for offline online
    #i.e. get rid of this complicated file reading stuff and config_header and just read the config as a dict
    if config.type == "atscale.offline":
        config_header = "connection.feast_offline_store" #honestly idk what this does - Ty

    config = dict(config) #turn into dict
    project_name = config['project']

    #read config file
    config_reader = configparser.ConfigParser()
    config_reader.read([config["config_path"]]) #read actual config file
    if config_reader.has_section(config_header):
        kwargs = dict(config_reader[config_header])
    else:
        kwargs = {}

    kwargs.update((k, v) for k, v in config.items())

    try:
        atproject = atscale.atscale.AtScale(
            server=kwargs["server"],
            organization=kwargs["organization"],
            project_id=kwargs["project_id"],
            model_id=kwargs["model_id"],
            token=kwargs["token"],
            username=kwargs["user"],
            password=kwargs["pw"],
            design_center_server_port=kwargs["design_center_server_port"],
            engine_port=kwargs["engine_port"],
        )
    except UserError as e:
        raise FeastProviderLoginError(str(e))

    connection_reqs: dict[str, list[str]] = {
        'redshift': ['username', 'password',
                      'host', 'port', 'database',
                      'schema'],
        'iris': ['user', 'host', 'namespace', 'driver',
                 'port', 'schema', 'password'],
        'databricks': ['token', 'host'],
        'bigquery': ['credentials_path', 'project', 'dataset'],
        'snowflake': ['username', 'account', 'warehouse',
                      'database', 'schema', 'password'],
        'synapse': ['username', 'host', 'database', 'schema',
                    'driver', 'port', 'password']
    }

    def password():
        if config['password'] is None:
            return getpass.getpass(f'{conn_id} password for user: {config["username"]}')
        else:
            return config['password']
        #doesn't write newfound password to file

    conn_id = config['connection_id']
    if conn_id is None:
        raise UserError("You must create a db connection,"
                        " or pass its credentials to the feast config,"
                        " to use an AtScale project as a feast project.")
    else:
        for param in connection_reqs[conn_id]:
            if config[param] is None:
                raise UserError(f'Incomplete config for {conn_id}'
                                f' connection, missing {param}')
        #todo: put password in constructors in db.Iris and others
        constructors = {
            'redshift': atscale.db.Redshift(atscale_connection_id=f'feast-{project_name}',
                                            username=kwargs['username'],
                                            password=password(),
                                            host=kwargs['host'],
                                            database_name=kwargs['database'],
                                            schema=kwargs['schema'],
                                            port=kwargs['port']),
            'iris': atscale.db.Iris(atscale_connection_id=f'feast-{project_name}',
                                    username=kwargs['username'],
                                    host=kwargs['host'],
                                    namespace=kwargs['namespace'],
                                    driver=kwargs['driver'],
                                    schema=kwargs['schema'],
                                    port=kwargs['port']
                                    ),
            'databricks': atscale.db.Databricks(atscale_connection_id=f'feast-{project_name}',
                                                token=kwargs['token'],
                                                host=kwargs['host'],
                                                database=kwargs['database'],
                                                http_path=kwargs['http_path'],
                                                driver=kwargs['driver'],
                                                port=kwargs['port']),
            'synapse': atscale.db.Synapse(atscale_connection_id=f'feast-{project_name}',
                                          username=kwargs['username'],
                                          host=kwargs['host'],
                                          database=kwargs['database'],
                                          driver=kwargs['driver'],
                                          schema=kwargs['schema'],
                                          port=kwargs['port']),
            'bigquery': atscale.db.BigQuery(atscale_connection_id=f'feast-{project_name}',
                                            credentials_path=kwargs['credentials_path'],
                                            project=kwargs['project'],
                                            dataset=kwargs['dataset']),
            'snowflake': atscale.db.Snowflake(atscale_connection_id=f'feast-{project_name}',
                                              username=kwargs['username'],
                                              password=password(),
                                              account=kwargs['account'],
                                              warehouse=kwargs['warehouse'],
                                              database=kwargs['database'],
                                              schema=kwargs['schema'])
        }
        print('Does not instantiate in dict')
        #todo check for redshift params
        atproject.create_db_connection(constructors[kwargs['connection_id']])
    return atproject

def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    atproject: AtScale,
) -> Tuple[datetime, datetime]:

    #entity_df will be projec_query_name.model_query_name since all features in atscale are not in tables

    if isinstance(entity_df, pd.DataFrame):
        entity_df_event_timestamp = entity_df.loc[
            :, entity_df_event_timestamp_col
        ].infer_objects()
        if pd.api.types.is_string_dtype(entity_df_event_timestamp):
            entity_df_event_timestamp = pd.to_datetime(
                entity_df_event_timestamp, utc=True
            )
        entity_df_event_timestamp_range = (
            entity_df_event_timestamp.min().to_pydatetime(),
            entity_df_event_timestamp.max().to_pydatetime(),
        )
    elif isinstance(entity_df, str):
        # If the entity_df is a string (SQL query), determine range
        # from table
        query = f'SELECT MIN("{entity_df_event_timestamp_col}") AS "min_value", ' \
                f'MAX("{entity_df_event_timestamp_col}") AS "max_value" ' \
                f'FROM ({entity_df})'
        results = atproject.custom_query(query) #atscale.database.submit_query?

        entity_df_event_timestamp_range = cast(Tuple[datetime, datetime], results[0])
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_df_event_timestamp_range

MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
WITH "entity_dataframe" AS (
    SELECT *,
        "{{entity_df_event_timestamp_col}}" AS "entity_timestamp"
        {% for featureview in featureviews %}
            {% if featureview.entities %}
            ,(
                {% for entity in featureview.entities %}
                    CAST("{{entity}}" AS VARCHAR) ||
                {% endfor %}
                CAST("{{entity_df_event_timestamp_col}}" AS VARCHAR)
            ) AS "{{featureview.name}}__entity_row_unique_id"
            {% else %}
            ,CAST("{{entity_df_event_timestamp_col}}" AS VARCHAR) AS "{{featureview.name}}__entity_row_unique_id"
            {% endif %}
        {% endfor %}
    FROM "{{ left_table_query_string }}"
),

{% for featureview in featureviews %}

"{{ featureview.name }}__entity_dataframe" AS (
    SELECT
        {{ featureview.entities | map('tojson') | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
        "entity_timestamp",
        "{{featureview.name}}__entity_row_unique_id"
    FROM "entity_dataframe"
    GROUP BY
        {{ featureview.entities | map('tojson') | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
        "entity_timestamp",
        "{{featureview.name}}__entity_row_unique_id"
),

/*
 This query template performs the point-in-time correctness join for a single feature set table
 to the provided entity table.

 1. We first join the current feature_view to the entity dataframe that has been passed.
 This JOIN has the following logic:
    - For each row of the entity dataframe, only keep the rows where the `event_timestamp_column`
    is less than the one provided in the entity dataframe
    - If there a TTL for the current feature_view, also keep the rows where the `event_timestamp_column`
    is higher the the one provided minus the TTL
    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
    computed previously

 The output of this CTE will contain all the necessary information and already filtered out most
 of the data that is not relevant.
*/

"{{ featureview.name }}__subquery" AS (
    SELECT
        "{{ featureview.event_timestamp_column }}" as "event_timestamp",
        {{'"' ~ featureview.created_timestamp_column ~ '" as "created_timestamp",' if featureview.created_timestamp_column else '' }}
        {{featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
        {% for feature in featureview.features %}
            "{{ feature }}" as {% if full_feature_names %}"{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }}
    WHERE "{{ featureview.event_timestamp_column }}" <= '{{ featureview.max_event_timestamp }}'
    {% if featureview.ttl == 0 %}{% else %}
    AND "{{ featureview.event_timestamp_column }}" >= '{{ featureview.min_event_timestamp }}'
    {% endif %}
),

"{{ featureview.name }}__base" AS (
    SELECT
        "subquery".*,
        "entity_dataframe"."entity_timestamp",
        "entity_dataframe"."{{featureview.name}}__entity_row_unique_id"
    FROM "{{ featureview.name }}__subquery" AS "subquery"
    INNER JOIN "{{ featureview.name }}__entity_dataframe" AS "entity_dataframe"
    ON TRUE
        AND "subquery"."event_timestamp" <= "entity_dataframe"."entity_timestamp"

        {% if featureview.ttl == 0 %}{% else %}
        AND "subquery"."event_timestamp" >= TIMESTAMPADD(second,-{{ featureview.ttl }},"entity_dataframe"."entity_timestamp")
        {% endif %}

        {% for entity in featureview.entities %}
        AND "subquery"."{{ entity }}" = "entity_dataframe"."{{ entity }}"
        {% endfor %}
),

/*
 2. If the `created_timestamp_column` has been set, we need to
 deduplicate the data first. This is done by calculating the
 `MAX(created_at_timestamp)` for each event_timestamp.
 We then join the data on the next CTE
*/
{% if featureview.created_timestamp_column %}
"{{ featureview.name }}__dedup" AS (
    SELECT
        "{{featureview.name}}__entity_row_unique_id",
        "event_timestamp",
        MAX("created_timestamp") AS "created_timestamp"
    FROM "{{ featureview.name }}__base"
    GROUP BY "{{featureview.name}}__entity_row_unique_id", "event_timestamp"
),
{% endif %}

/*
 3. The data has been filtered during the first CTE "*__base"
 Thus we only need to compute the latest timestamp of each feature.
*/
"{{ featureview.name }}__latest" AS (
    SELECT
        "event_timestamp",
        {% if featureview.created_timestamp_column %}"created_timestamp",{% endif %}
        "{{featureview.name}}__entity_row_unique_id"
    FROM
    (
        SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY "{{featureview.name}}__entity_row_unique_id"
                ORDER BY "event_timestamp" DESC{% if featureview.created_timestamp_column %},"created_timestamp" DESC{% endif %}
            ) AS "row_number"
        FROM "{{ featureview.name }}__base"
        {% if featureview.created_timestamp_column %}
            INNER JOIN "{{ featureview.name }}__dedup"
            USING ("{{featureview.name}}__entity_row_unique_id", "event_timestamp", "created_timestamp")
        {% endif %}
    )
    WHERE "row_number" = 1
),

/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
"{{ featureview.name }}__cleaned" AS (
    SELECT "base".*
    FROM "{{ featureview.name }}__base" AS "base"
    INNER JOIN "{{ featureview.name }}__latest"
    USING(
        "{{featureview.name}}__entity_row_unique_id",
        "event_timestamp"
        {% if featureview.created_timestamp_column %}
            ,"created_timestamp"
        {% endif %}
    )
){% if loop.last %}{% else %}, {% endif %}


{% endfor %}
/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */

SELECT "{{ final_output_feature_names | join('", "')}}"
FROM "entity_dataframe"
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        "{{featureview.name}}__entity_row_unique_id"
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}"{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}
        {% endfor %}
    FROM "{{ featureview.name }}__cleaned"
) "{{ featureview.name }}__cleaned" USING ("{{featureview.name}}__entity_row_unique_id")
{% endfor %}
"""