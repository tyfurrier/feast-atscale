import configparser
import contextlib
import getpass
import logging
import time
from datetime import datetime, timedelta
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

import atscale
import atscale.db
import numpy as np
import pandas as pd
import pyarrow
import pyarrow as pa
from atscale.errors import UserError
from pydantic import StrictStr
from pydantic.typing import Literal
from pytz import utc

from feast import AtScaleSource, OnDemandFeatureView
from feast.data_source import DataSource
from feast.errors import FeastProviderLoginError, InvalidEntityType
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_utils import FeatureViewQueryContext
from feast.infra.utils.atscale_utils import get_atscale_api
from feast.infra.offline_stores.atscale_source import SavedDatasetAtScaleStorage
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.usage import log_exceptions_and_usage
from feast.utils import to_naive_utc

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

        atproject = get_atscale_api(config.offline_store)
        features = join_key_columns + feature_name_columns + [event_timestamp_column]
        atscale_query = atproject.generate_atscale_query(features=features)
        atscale_query += f"""
         WHERE {event_timestamp_column} BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')"""
        # todo: look into using get_data filter_between
        return AtScaleRetrievalJob(
            atproject=atproject, query=atscale_query, full_feature_names=False,
        )

    @staticmethod
    @log_exceptions_and_usage(offline_store="atscale")
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        logging.info("Getting historical features from atscale offline store")

        assert isinstance(config.offline_store, AtScaleOfflineStoreConfig)

        atproject: AtScale = get_atscale_api(config.offline_store)

        entity_schema: dict[str, np.dtype] = _get_entity_schema(entity_df, atproject)
        # dict from column to datatype

        entity_df_event_timestamp_col = offline_utils.infer_event_timestamp_from_entity_df(
            entity_schema
        )  # takes datetime dtype as timestamp col

        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df, entity_df_event_timestamp_col, atproject,
        )

        # todo try to do this without creating entity df in database
        # or not, don't reinvent the wheel ppl will still appreciate you ;)
        @contextlib.contextmanager
        def query_generator() -> Iterator[str]:

            # random name for new table
            table_name: str = 'feast_ent_df847433206'

            _upload_entity_df(
                entity_df=entity_df, table_name=table_name, atproject=atproject
            )

            expected_join_keys = offline_utils.get_expected_join_keys(
                project, feature_views, registry
            )

            offline_utils.assert_expected_columns_in_entity_df(
                entity_schema, expected_join_keys, entity_df_event_timestamp_col
            )

            def get_feature_view_query_context(
                    feature_refs: List[str],
                    feature_views: List[FeatureView],
                    registry: Registry,
                    project: str,
                    entity_df_timestamp_range: Tuple[datetime, datetime],
            ) -> List[FeatureViewQueryContext]:

                """Build a query context containing all information required to template a BigQuery and Redshift point-in-time SQL query"""

                (
                    feature_views_to_feature_map,
                    on_demand_feature_views_to_features,
                ) = offline_utils._get_requested_feature_views_to_features_dict(
                    feature_refs, feature_views, registry.list_on_demand_feature_views(project)
                )

                query_context = []
                for feature_view, features in feature_views_to_feature_map.items():
                    join_keys = []
                    entity_selections = []
                    reverse_field_mapping = {
                        v: k for k, v in feature_view.batch_source.field_mapping.items()
                    }
                    for entity_name in feature_view.entities:
                        entity = registry.get_entity(entity_name, project)
                        join_key = feature_view.projection.join_key_map.get(
                            entity.join_key, entity.join_key
                        )
                        join_keys.append(join_key)
                        entity_selections.append(f'"{entity.join_key}" AS "{join_key}"')

                    if isinstance(feature_view.ttl, timedelta):
                        ttl_seconds = int(feature_view.ttl.total_seconds())
                    else:
                        ttl_seconds = 0

                    event_timestamp_column = feature_view.batch_source.event_timestamp_column
                    created_timestamp_column = feature_view.batch_source.created_timestamp_column

                    min_event_timestamp = None
                    if feature_view.ttl:
                        min_event_timestamp = to_naive_utc(
                            entity_df_timestamp_range[0] - feature_view.ttl
                        ).isoformat()

                    max_event_timestamp = to_naive_utc(entity_df_timestamp_range[1]).isoformat()

                    all_in_subquery = ([reverse_field_mapping.get(feature, feature) for feature in features] +
                                       [reverse_field_mapping.get(event_timestamp_column, event_timestamp_column)] +
                                       join_keys)
                    if created_timestamp_column:
                        all_in_subquery.append(reverse_field_mapping.get(created_timestamp_column,
                                                                         created_timestamp_column))

                    context = FeatureViewQueryContext(
                        name=feature_view.projection.name_to_use(),
                        ttl=ttl_seconds,
                        entities=join_keys,
                        features=[
                            reverse_field_mapping.get(feature, feature) for feature in features
                        ],
                        field_mapping=feature_view.batch_source.field_mapping,
                        event_timestamp_column=reverse_field_mapping.get(
                            event_timestamp_column, event_timestamp_column
                        ),
                        created_timestamp_column=reverse_field_mapping.get(
                            created_timestamp_column, created_timestamp_column
                        ),
                        # Make created column optional and not hardcoded
                        table_subquery=atproject.generate_db_query(
                            atscale_query=atproject.generate_atscale_query(features=all_in_subquery)),
                        entity_selections=entity_selections,
                        min_event_timestamp=min_event_timestamp,
                        max_event_timestamp=max_event_timestamp,
                    )
                    query_context.append(context)
                return query_context

            # Build a query context containing all information required to template the SQL query
            query_context = get_feature_view_query_context(
                feature_refs,
                feature_views,
                registry,
                project,
                entity_df_event_timestamp_range,
            )


            feature_view = feature_views[0]
            start = time.time()
            while table_name == 'feast_ent_df847433206':
                try:
                    end = time.time()
                    table_name = atproject.generate_db_query(f"""SELECT {', '.join(
                        [f'`{table_name}_{col}` AS "{col}"' for col in entity_df.columns])} 
                        FROM `PYTEST Walmart`.`m5_walmart_sales` `m5_walmart_sales`""")
                except Exception:
                    if end - start > 120:
                        raise

            # Generate the AtScale query query from the query context
            query = offline_utils.build_point_in_time_query(
                query_context,
                left_table_query_string=table_name,  # new generated table
                entity_df_event_timestamp_col=entity_df_event_timestamp_col,
                entity_df_columns=entity_schema.keys(),
                query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
                full_feature_names=full_feature_names,
            )

            yield query

        return AtScaleRetrievalJob(
            query=query_generator,
            atproject=atproject,
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

    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
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

        atproject = get_atscale_api(config.offline_store)

        feature_name_columns = [
            f"`{atproject.model_name}`.`{f}`" for f in feature_name_columns
        ]
        if join_key_columns:
            partition_by_join_key_string = "PARTITION BY " + ", ".join(
                f"`{atproject.model_name}`.`{key}`" for key in join_key_columns
            )
        else:
            partition_by_join_key_string = ""

        timestamp_columns = [f"`{atproject.model_name}`.`{event_timestamp_column}`"]
        if created_timestamp_column:
            timestamp_columns.append(
                f"`{atproject.model_name}`.`{created_timestamp_column}`"
            )

        timestamp_desc_string = " DESC, ".join(timestamp_columns) + " DESC"
        field_string = ", ".join(
            join_key_columns + feature_name_columns + timestamp_columns
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
            atproject=atproject, query=query, full_feature_names=False,
        )


class AtScaleRetrievalJob(RetrievalJob):
    def __init__(
        self,
        atproject: AtScale,
        query: Union[str, Callable[[], ContextManager[str]]],
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        if not isinstance(query, str):
            self._query_generator = query
        else:

            @contextlib.contextmanager
            def query_generator() -> Iterator[str]:
                assert isinstance(query, str)
                yield query

            self._query_generator = query_generator
        self.atproject = atproject
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
            df = self._execute_sql(query)
            return df

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
        self.atproject.database.add_table(table_name=table_name, dataframe=self.to_df())

    def persist(self, storage: SavedDatasetStorage):
        assert isinstance(storage, SavedDatasetAtScaleStorage)

        if storage.table_ref is None:
            raise Exception(
                "table_ref must have some value for the SavedDatasetStorage parameter"
            )
        else:
            prefix = storage.table_ref

        result_df = self.to_df()

        self.atproject.add_table(prefix, result_df, [], if_exists="append")

        features = []
        for feat in result_df.columns:
            features.append(f"{prefix}_{feat}")
            self.atproject.create_aggregate_feature(
                dataset_name=prefix, column=feat, name=f"{prefix}_{feat}", folder=prefix
            )
        storage.atscale_options.features += features

    def _execute_sql(self, query) -> pd.DataFrame:
        """ Executes a query after its translation by the AtScale query planner"""
        return self.atproject.database.submit_query(db_query=query)


class AtScaleOfflineStoreConfig(FeastConfigBaseModel):
    """ Custom offline store config for AtScale"""

    type: Literal["atscale.offline"] = "atscale.offline"
    """ Offline store type selector"""

    server: StrictStr

    token: Optional[StrictStr]

    username: Optional[StrictStr]

    password: Optional[StrictStr]

    organization: StrictStr

    project_id: StrictStr

    model_id: StrictStr

    design_center_server_port: StrictStr = "10500"

    engine_port: StrictStr = "10502"

    """Database configuration options
    (at least three aren't optional tho u rly need it, stay strapped ;)"""

    db_connection_id: StrictStr
    """ used to ensure this is a unique connection, if there is any db connection in this project using the same
    atscale_connection_id, then an error will be thrown as the connection already exists"""

    db_type: StrictStr
    """ data source db type can be 'redshift', 'iris', 'snowflake', 'synapse', 'bigquery', 'databricks' """

    db_username: Optional[StrictStr]
    """Redshift, iris, Snowflake, Synapse"""

    db_password: Optional[StrictStr]
    """Redshift, iris, Snowflake, Synapse"""

    db_host: Optional[StrictStr]
    """Redshift, iris, Databricks, Host"""

    db_driver: Optional[StrictStr]
    """Iris, Databricks, Synapse"""

    db_port: Optional[StrictStr]
    """Redshift, Iris, Databricks, Synapse"""

    db_database: Optional[StrictStr]
    """Redshift, Snowflake, Synapse"""

    db_schema: Optional[StrictStr]
    """Redshift, Iris, Databricks, Snowflake, Synapse"""

    """bigquery parameters"""
    db_credentials_path: Optional[StrictStr]

    db_project: Optional[StrictStr]

    db_dataset: Optional[StrictStr]

    """snowflake"""
    db_account: Optional[StrictStr]

    db_warehouse: Optional[StrictStr]

    """databricks"""
    db_token: Optional[StrictStr]

    db_http_path: Optional[StrictStr]

    """iris"""
    db_namespace: Optional[StrictStr]


def _get_entity_schema(
    entity_df: Union[pd.DataFrame, str], atscale_connection: AtScale
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, str):
        entity_df_sample = atscale_connection.custom_query(
            f"SELECT * FROM ({entity_df}) LIMIT 1"
        )
        entity_schema = dict(zip(entity_df_sample.columns, entity_df_sample.dtypes))
    elif isinstance(entity_df, pd.DataFrame):
        entity_schema = dict(zip(entity_df.columns, entity_df.dtypes))
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_schema


def _upload_entity_df(
    entity_df: Union[pd.DataFrame, str], atproject: AtScale, table_name: str,
) -> None:
    """table_name parameter does nothing, it will always be named the same thing and replace previous entity df's"""
    if isinstance(entity_df, str):
        atproject.database.execute_statement(
            f'CREATE OR REPLACE TABLE "{atproject.database.fix_table_name(table_name=table_name)}" AS ({entity_df})'
        )
        entity_df = atproject.database.submit_query(
            db_query=f'SELECT * FROM "{atproject.database.fix_table_name(table_name=table_name)}"')
    elif not isinstance(entity_df, pd.DataFrame):
        raise InvalidEntityType(type(entity_df))
    try:
        atproject.delete_dataset(table_name)
    except Exception as e:
        print('DATASET DELETION', str(e))
    try:
        atproject.database.execute_statement(db_statement=f'DROP TABLE IF EXISTS {table_name}')
    except Exception as e:
        print('DROPPING TABLE', str(e))
    atproject.add_table(table_name=table_name,
                        dataframe=entity_df,
                        join_features=[],
                        join_columns=[],
                        if_exists='fail')
    for col in entity_df.columns:
        if entity_df[col].dtypes.name in ['int64', 'float64']:
            try:
                atproject.create_aggregate_feature(dataset_name=atproject.database.fix_table_name(table_name=table_name),
                                                   column=atproject.database.fix_table_name(col),
                                                   name=f'{table_name}_{col}',
                                                   aggregation_type='SUM',
                                                   folder=table_name)
            except Exception as e:
                atproject.create_aggregate_feature(
                    dataset_name=atproject.database.fix_table_name(table_name=table_name),
                    column=atproject.database.fix_table_name(col),
                    name=f'{table_name}_{col}',
                    aggregation_type='SUM',
                    folder=table_name)
                #todo: check if col name is the same (only not if different db's, different fix_table_name func)
        else:
            try:
                atproject.create_denormalized_categorical_feature(dataset_name=atproject.database.fix_table_name(
                    table_name=table_name),
                    column=atproject.database.fix_table_name(table_name=col),
                    name=f'feast_ent_df847433206_{col}',
                    folder=table_name)
            except Exception as e:
                raise Exception(f'creating categorical {e}')
                #todo delete feature and try again


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    atproject: AtScale,
) -> Tuple[datetime, datetime]:

    # entity_df will be projec_query_name.model_query_name since all features in atscale are not in tables

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
        query = (
            f'SELECT MIN("{entity_df_event_timestamp_col}") AS "min_value", '
            f'MAX("{entity_df_event_timestamp_col}") AS "max_value" '
            f"FROM ({entity_df})"
        )
        results = atproject.custom_query(query)  # atscale.database.submit_query?

        entity_df_event_timestamp_range = cast(Tuple[datetime, datetime], results[0])
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_df_event_timestamp_range


ENTITY_DF = """
SELECT 
    *,
    {{entity_df_event_timestamp_col}} AS "entity_timestamp"
    {% for featureview in featureviews %}
        {% if featureview.entities %}
        ,CONCAT(
            {% for entity in featureview.entities %}
                CAST("{{entity}}" AS VARCHAR(64)), 
            {% endfor %}
            CAST("{{entity_df_event_timestamp_col}}" AS VARCHAR(64))
        ) AS "{{featureview.name}}__entity_row_unique_id"
        {% else %}
        ,CAST("{{entity_df_event_timestamp_col}}" AS VARCHAR(64)) AS "{{featureview.name}}__entity_row_unique_id"
        {% endif %}
    {% endfor %}
FROM {{ left_table_query_string }} AS "atscale_entity_df"
"""

FV_ENTITY_DF = """SELECT
                        "{{ featureview.entities | join('", "')}}{% if featureview.entities %}",{% else %}{% endif %}
                        "entity_timestamp",
                        "{{featureview.name}}__entity_row_unique_id"
                    FROM 
                            (""" + ENTITY_DF + """
                            ) AS "entity_dataframe"
                    GROUP BY
                        "{{ featureview.entities | join('", "')}}{% if featureview.entities %}",{% else %}{% endif %}
                        "entity_timestamp",
                        "{{featureview.name}}__entity_row_unique_id"
                """

SUBQUERY = """SELECT
                    `{{ featureview.event_timestamp_column }}` as "event_timestamp",
                    {{'"' ~ featureview.created_timestamp_column ~ '" as "created_timestamp",' if featureview.created_timestamp_column else '' }}
                    {{featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
                    {% for feature in featureview.features %}
                        `{{ feature }}` as {% if full_feature_names %}"{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}{% if loop.last %}{% else %}, {% endif %}
                    {% endfor %}
                FROM {{ featureview.table_subquery }}
                WHERE `{{ featureview.event_timestamp_column }}` <= '{{ featureview.max_event_timestamp }}'
                {% if featureview.ttl == 0 %}{% else %}
                AND `{{ featureview.event_timestamp_column }}` >= '{{ featureview.min_event_timestamp }}'
                {% endif %}
            """

BASE = """SELECT
                        "subquery".*,
                        "entity_dataframe"."entity_timestamp",
                        "entity_dataframe"."{{featureview.name}}__entity_row_unique_id"
                    FROM 
                    ("""+ SUBQUERY + """
                    ) AS "subquery"
                    INNER JOIN 
                    (""" + FV_ENTITY_DF + """
                    ) AS "{{ featureview.name }}__entity_dataframe"
                    ON TRUE
                        AND "subquery"."event_timestamp" <= "entity_dataframe"."entity_timestamp"
                
                        {% if featureview.ttl == 0 %}{% else %}
                        AND "subquery"."event_timestamp" >= TIMESTAMPADD(second,-{{ featureview.ttl }},"entity_dataframe"."entity_timestamp")
                        {% endif %}
                
                        {% for entity in featureview.entities %}
                        AND "subquery"."{{ entity }}" = "entity_dataframe"."{{ entity }}"
                        {% endfor %}
                        """

DEDUP = """SELECT
                                        "{{featureview.name}}__entity_row_unique_id",
                                        "event_timestamp",
                                        MAX("created_timestamp") AS "created_timestamp"
                                    FROM 
                                    (""" + BASE + """    
                                    ) AS "{{ featureview.name }}__base"
                                    GROUP BY "{{featureview.name}}__entity_row_unique_id", "event_timestamp"
                                    """

LATEST = """SELECT
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
                        FROM 
                        (""" + BASE + """
                        ) AS "{{ featureview.name }}__base"
                        {% if featureview.created_timestamp_column %}
                            INNER JOIN 
                            (""" + DEDUP + """
                            ) AS "{{ featureview.name }}__dedup"
                            USING ("{{featureview.name}}__entity_row_unique_id", "event_timestamp", "created_timestamp")
                        {% endif %}
                    ) AS "special_sauce"
                    WHERE "row_number" = 1"""

CLEANED = """SELECT "base".*
            FROM 
            (""" + BASE + """
            ) AS "base"
            INNER JOIN 
            (""" + LATEST + """
            ) AS "{{ featureview.name }}__latest"
            USING(
                "{{featureview.name}}__entity_row_unique_id",
                "event_timestamp"
                {% if featureview.created_timestamp_column %}
                    ,"created_timestamp"
                {% endif %}
            )"""

FINAL = """SELECT "{{ final_output_feature_names | join('", "')}}"
FROM 
        (""" + ENTITY_DF + """
        ) AS "entity_dataframe"
{% for featureview in featureviews %}
LEFT JOIN 
(
    SELECT
        "{{featureview.name}}__entity_row_unique_id"
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}"{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}
        {% endfor %}
    FROM 
    (""" + CLEANED + """
    ) AS "{{ featureview.name }}__cleaned"
) "{{ featureview.name }}__cleaned" USING ("{{featureview.name}}__entity_row_unique_id")
{% endfor %}"""


MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN_V1 = FINAL



MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN_OLD =  """
SELECT "{{ final_output_feature_names | join('", "')}}"
FROM 
        (
        SELECT 
            `feast_ent_df847433206_{{ featureview.entities | join('`, `feast_ent_df847433206_')}}{% if featureview.entities %}`,{% else %}{% endif %}
            `feast_ent_df847433206_{{entity_df_event_timestamp_col}}` AS "entity_timestamp"
            {% for featureview in featureviews %}
                {% if featureview.entities %}
                ,CONCAT(
                    {% for entity in featureview.entities %}
                        CAST(`feast_ent_df847433206_{{entity}}` AS VARCHAR(64)), 
                    {% endfor %}
                    CAST(`feast_ent_df847433206_{{entity_df_event_timestamp_col}}` AS VARCHAR(64))
                ) AS "{{featureview.name}}__entity_row_unique_id"
                {% else %}
                ,CAST(`feast_ent_df847433206_{{entity_df_event_timestamp_col}}` AS VARCHAR(64)) AS "{{featureview.name}}__entity_row_unique_id"
                {% endif %}
                {% for column in 
            {% endfor %}
        FROM {{ featureviews[0].table_subquery }}
        ) AS "entity_dataframe"
{% for featureview in featureviews %}
LEFT JOIN 
(
    SELECT
        "{{featureview.name}}__entity_row_unique_id"
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}"{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}
        {% endfor %}
    FROM 
    (
            SELECT "base".*
            FROM 
            (
                    SELECT
                        "subquery".*,
                        "entity_dataframe"."entity_timestamp",
                        "entity_dataframe"."{{featureview.name}}__entity_row_unique_id"
                    FROM 
                    (
                            SELECT
                                `{{ featureview.event_timestamp_column }}` as "event_timestamp",
                                {{'"' ~ featureview.created_timestamp_column ~ '" as "created_timestamp",' if featureview.created_timestamp_column else '' }}
                                {{featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
                                {% for feature in featureview.features %}
                                    `{{ feature }}` as {% if full_feature_names %}"{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}{% if loop.last %}{% else %}, {% endif %}
                                {% endfor %}
                            FROM {{ featureview.table_subquery }}
                            WHERE `{{ featureview.event_timestamp_column }}` <= '{{ featureview.max_event_timestamp }}'
                            {% if featureview.ttl == 0 %}{% else %}
                            AND `{{ featureview.event_timestamp_column }}` >= '{{ featureview.min_event_timestamp }}'
                            {% endif %}
                    ) AS "subquery"
                    INNER JOIN 
                    (
                            SELECT
                                "feast_ent_df847433206_{{ featureview.entities | join('", "feast_ent_df847433206_')}}{% if featureview.entities %}",{% else %}{% endif %}
                                "entity_timestamp",
                                "{{featureview.name}}__entity_row_unique_id"
                            FROM 
                                    (
                                    SELECT 
                                        `feast_ent_df847433206_{{ featureview.entities | join('`, `feast_ent_df847433206_')}}{% if featureview.entities %}`,{% else %}{% endif %}
                                        `feast_ent_df847433206_{{entity_df_event_timestamp_col}}` AS "entity_timestamp"
                                        {% for featureview in featureviews %}
                                            {% if featureview.entities %}
                                            ,CONCAT(
                                                {% for entity in featureview.entities %}
                                                    CAST(`feast_ent_df847433206_{{entity}}` AS VARCHAR(64)),
                                                {% endfor %}
                                                CAST(`feast_ent_df847433206_{{entity_df_event_timestamp_col}}` AS VARCHAR(64))
                                            ) AS "{{featureview.name}}__entity_row_unique_id"
                                            {% else %}
                                            ,CAST(`feast_ent_df847433206_{{entity_df_event_timestamp_col}}` AS VARCHAR(64)) AS "{{featureview.name}}__entity_row_unique_id"
                                            {% endif %}
                                        {% endfor %}
                                    FROM {{ featureview.table_subquery }}
                                    ) AS "entity_dataframe"
                            GROUP BY
                                "feast_ent_df847433206_{{ featureview.entities | join('", "feast_ent_df847433206_')}}{% if featureview.entities %}",{% else %}{% endif %}
                                "entity_timestamp",
                                "{{featureview.name}}__entity_row_unique_id"
                    ) AS "{{ featureview.name }}__entity_dataframe"
                    ON TRUE
                        AND "subquery"."event_timestamp" <= "entity_dataframe"."entity_timestamp"
                
                        {% if featureview.ttl == 0 %}{% else %}
                        AND "subquery"."event_timestamp" >= TIMESTAMPADD(second,-{{ featureview.ttl }},"entity_dataframe"."entity_timestamp")
                        {% endif %}
                
                        {% for entity in featureview.entities %}
                        AND "subquery"."{{ entity }}" = "entity_dataframe"."{{ entity }}"
                        {% endfor %}
            ) AS "base"
            INNER JOIN 
            (
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
                        FROM 
                        (
                                SELECT
                                    "subquery".*,
                                    "entity_dataframe"."entity_timestamp",
                                    "entity_dataframe"."{{featureview.name}}__entity_row_unique_id"
                                FROM 
                                (
                                        SELECT
                                            `{{ featureview.event_timestamp_column }}` as "event_timestamp",
                                            {{'"' ~ featureview.created_timestamp_column ~ '" as "created_timestamp",' if featureview.created_timestamp_column else '' }}
                                            {{featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
                                            {% for feature in featureview.features %}
                                                `{{ feature }}` as {% if full_feature_names %}"{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}{% if loop.last %}{% else %}, {% endif %}
                                            {% endfor %}
                                        FROM {{ featureview.table_subquery }}
                                        WHERE `{{ featureview.event_timestamp_column }}` <= '{{ featureview.max_event_timestamp }}'
                                        {% if featureview.ttl == 0 %}{% else %}
                                        AND `{{ featureview.event_timestamp_column }}` >= '{{ featureview.min_event_timestamp }}'
                                        {% endif %}
                                ) AS "subquery"
                                INNER JOIN 
                                (
                                        SELECT
                                            "feast_ent_df847433206_{{ featureview.entities | join('", "feast_ent_df847433206_')}}{% if featureview.entities %}",{% else %}{% endif %}
                                            "entity_timestamp",
                                            "{{featureview.name}}__entity_row_unique_id"
                                        FROM 
                                        (
                                                SELECT 
                                                    `feast_ent_df847433206_{{ featureview.entities | join('`, `feast_ent_df847433206_')}}{% if featureview.entities %}`,{% else %}{% endif %}
                                                    `feast_ent_df847433206_{{entity_df_event_timestamp_col}}` AS "entity_timestamp"
                                                    {% for featureview in featureviews %}
                                                        {% if featureview.entities %}
                                                        ,CONCAT(
                                                            {% for entity in featureview.entities %}
                                                                CAST(`feast_ent_df847433206_{{entity}}` AS VARCHAR(64)),
                                                            {% endfor %}
                                                            CAST(`feast_ent_df847433206_{{entity_df_event_timestamp_col}}` AS VARCHAR(64))
                                                        ) AS "{{featureview.name}}__entity_row_unique_id"
                                                        {% else %}
                                                        ,CAST(`feast_ent_df847433206_{{entity_df_event_timestamp_col}}` AS VARCHAR(64)) AS "{{featureview.name}}__entity_row_unique_id"
                                                        {% endif %}
                                                    {% endfor %}
                                                FROM {{ featureview.table_subquery }}
                                        ) AS "entity_dataframe"
                                        GROUP BY
                                            "feast_ent_df847433206_{{ featureview.entities | join('", "feast_ent_df847433206_')}}{% if featureview.entities %}",{% else %}{% endif %}
                                            "entity_timestamp",
                                            "{{featureview.name}}__entity_row_unique_id"
                                ) AS "{{ featureview.name }}__entity_dataframe"
                                ON TRUE
                                    AND "subquery"."event_timestamp" <= "entity_dataframe"."entity_timestamp"
                            
                                    {% if featureview.ttl == 0 %}{% else %}
                                    AND "subquery"."event_timestamp" >= TIMESTAMPADD(second,-{{ featureview.ttl }},"entity_dataframe"."entity_timestamp")
                                    {% endif %}
                            
                                    {% for entity in featureview.entities %}
                                    AND "subquery"."{{ entity }}" = "entity_dataframe"."{{ entity }}"
                                    {% endfor %}
                        ) AS "{{ featureview.name }}__base"
                        {% if featureview.created_timestamp_column %}
                            INNER JOIN 
                            (
                                    SELECT
                                        "{{featureview.name}}__entity_row_unique_id",
                                        "event_timestamp",
                                        MAX("created_timestamp") AS "created_timestamp"
                                    FROM 
                                    (
                                            SELECT
                                                "subquery".*,
                                                "entity_dataframe"."entity_timestamp",
                                                "entity_dataframe"."{{featureview.name}}__entity_row_unique_id"
                                            FROM 
                                            (
                                                    SELECT
                                                        `{{ featureview.event_timestamp_column }}` as "event_timestamp",
                                                        {{'"' ~ featureview.created_timestamp_column ~ '" as "created_timestamp",' if featureview.created_timestamp_column else '' }}
                                                        {{featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
                                                        {% for feature in featureview.features %}
                                                            `{{ feature }}` as {% if full_feature_names %}"{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}{% if loop.last %}{% else %}, {% endif %}
                                                        {% endfor %}
                                                    FROM {{ featureview.table_subquery }}
                                                    WHERE `{{ featureview.event_timestamp_column }}` <= '{{ featureview.max_event_timestamp }}'
                                                    {% if featureview.ttl == 0 %}{% else %}
                                                    AND `{{ featureview.event_timestamp_column }}` >= '{{ featureview.min_event_timestamp }}'
                                                    {% endif %}
                                            ) AS "subquery"
                                            INNER JOIN 
                                            (
                                                    SELECT
                                                        "feast_ent_df847433206_{{ featureview.entities | join('", "feast_ent_df847433206_')}}{% if featureview.entities %}",{% else %}{% endif %}
                                                        "entity_timestamp",
                                                        "{{featureview.name}}__entity_row_unique_id"
                                                    FROM 
                                                    (
                                                            SELECT 
                                                                `feast_ent_df847433206_{{ featureview.entities | join('`, `feast_ent_df847433206_')}}{% if featureview.entities %}`,{% else %}{% endif %}",
                                                                `feast_ent_df847433206_{{entity_df_event_timestamp_col}}` AS "entity_timestamp"
                                                                {% for featureview in featureviews %}
                                                                    {% if featureview.entities %}
                                                                    ,CONCAT(
                                                                        {% for entity in featureview.entities %}
                                                                            CAST(`feast_ent_df847433206_{{entity}}` AS VARCHAR(64)),
                                                                        {% endfor %}
                                                                        CAST(`feast_ent_df847433206_{{entity_df_event_timestamp_col}}` AS VARCHAR(64))
                                                                    ) AS "{{featureview.name}}__entity_row_unique_id"
                                                                    {% else %}
                                                                    ,CAST(`feast_ent_df847433206_{{entity_df_event_timestamp_col}}` AS VARCHAR(64)) AS "{{featureview.name}}__entity_row_unique_id"
                                                                    {% endif %}
                                                                {% endfor %}
                                                            FROM {{ featureview.table_subquery }}
                                                    ) AS "entity_dataframe"
                                                    GROUP BY
                                                        "feast_ent_df847433206_{{ featureview.entities | join('", "feast_ent_df847433206_')}}{% if featureview.entities %}",{% else %}{% endif %}
                                                        "entity_timestamp",
                                                        "{{featureview.name}}__entity_row_unique_id"
                                            ) AS "{{ featureview.name }}__entity_dataframe"
                                            ON TRUE
                                                AND "subquery"."event_timestamp" <= "entity_dataframe"."entity_timestamp"
                                        
                                                {% if featureview.ttl == 0 %}{% else %}
                                                AND "subquery"."event_timestamp" >= TIMESTAMPADD(second,-{{ featureview.ttl }},"entity_dataframe"."entity_timestamp")
                                                {% endif %}
                                        
                                                {% for entity in featureview.entities %}
                                                AND "subquery"."{{ entity }}" = "entity_dataframe"."{{ entity }}"
                                                {% endfor %}
                                    ) AS "{{ featureview.name }}__base"
                                    GROUP BY "{{featureview.name}}__entity_row_unique_id", "event_timestamp"
                            ) AS "{{ featureview.name }}__dedup"
                            USING ("{{featureview.name}}__entity_row_unique_id", "event_timestamp", "created_timestamp")
                        {% endif %}
                    ) AS "special_sauce"
                    WHERE "row_number" = 1
            ) AS "{{ featureview.name }}__latest"
            USING(
                "{{featureview.name}}__entity_row_unique_id",
                "event_timestamp"
                {% if featureview.created_timestamp_column %}
                    ,"created_timestamp"
                {% endif %}
            )
    ) AS "{{ featureview.name }}__cleaned"
) "{{ featureview.name }}__cleaned" USING ("{{featureview.name}}__entity_row_unique_id")
{% endfor %}
"""

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
    FROM ({{ left_table_query_string }})
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
    FROM ({{ featureview.table_subquery }})
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