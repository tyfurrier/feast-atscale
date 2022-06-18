from typing import Callable, Dict, Iterable, Optional, Tuple

from feast import type_map
from feast.data_source import DataSource
from feast.infra.utils.atscale_utils import get_atscale_api
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage, SavedDatasetStorageProto
from feast.value_type import ValueType
from atscale.utils.feast_utils import get_feast_value_types


class AtScaleSource(
    DataSource
):  # todo allow for limiting to certain features, hierarchies,
    def __init__(
        self,
        name: Optional[str] = None,
        features: Optional[list[str]] = None,
        event_timestamp_column: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
    ):
        """
        Creates an AtScaleSource object.

        Args:
            event_timestamp_column (optional): Event timestamp column used for point in
                time joins of feature values.
            query (optional): The query to be executed to obtain the features.
            created_timestamp_column (optional): Timestamp column indicating when the
                row was created, used for deduplicating rows.
            field_mapping (optional): A dictionary mapping of column names in this data
                source to column names in a feature table or view.
            date_partition_column (optional): Timestamp column used for partitioning.

        """
        super().__init__(
            name if name else "",
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            date_partition_column=date_partition_column,
        )
        self.atscale_options: AtScaleOptions = AtScaleOptions(features=features)

    def __eq__(self, other):
        if not isinstance(other, AtScaleSource):
            raise TypeError(
                "Equality comparisons should only be between AtScaleSource objects"
            )

        return (
            self.features == other.features
            and self.event_timestamp_column == other.event_timestamp_column
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        """
        Creates an AtScaleSource from protobuf representation of one

        Args:
            data_source: A protobuf representation of an AtScaleSource

        Returns:
            An AtScaleSource object based on the data_source
        """
        return AtScaleSource(
            features=data_source.atscale_options.features.split(", "),
            event_timestamp_column=data_source.event_timestamp_column,
            field_mapping=dict(data_source.field_mapping),
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
        )

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.BATCH_ATSCALE,
            field_mapping=self.field_mapping,
            atscale_options=self.atscale_options.to_proto(),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        """Checks that each feature can be individually queried, does not check if timestamp exists or if one
        can be inferred"""
        # todo: check full query, try timestamp inference upon timestamp being None
        self.get_table_column_names_and_types(config)

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        """
        Returns the callable method that returns Feast type given the raw column type.
        """
        return type_map.atscale_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        """
        Returns the list of column names and feast column types.

        Args:
            config: Configuration object used to configure a feature store.
        """
        from atscale.atscale import AtScale

        atproject: AtScale = get_atscale_api(config.offline_store)

        description = []
        features = self.features
        if features is None or not features:
            features = atproject.list_all_features()
        description = get_feast_value_types(project=atproject, features=features)
        return description

    def get_table_query_string(self, config: RepoConfig) -> str:
        """
        Returns a string that can directly be used to reference this set of features in SQL.
        """
        from atscale import AtScale

        atproject: AtScale = get_atscale_api(config.offline_store)

        return f'`{atproject.project_name}`.`{atproject.model_name}` `{atproject.model_name}`'

    @property
    def features(self) -> list[str]:
        return self.atscale_options.features


class AtScaleOptions:
    """
    DataSource AtScale options used to source features from AtScale query.
    """

    def __init__(self, features: Optional[list[str]], table_ref: Optional[str] = None):
        self._features = features
        self._table_ref = table_ref

    @property
    def features(self):
        """Returns the atscale features referenced by this source."""
        if self._features is None:
            return []
        else:
            return self._features

    @features.setter
    def features(self, features):
        """Sets the snowflake SQL query referenced by this source."""
        self._features: list[str] = features

    @property
    def table_ref(self):
        """Returns the name of the atscale fact table and db table that these
        queries should be sourced from"""
        return self._table_ref

    @classmethod
    def from_proto(cls, atscale_options_proto: DataSourceProto.AtScaleOptions):
        """
        Creates a SnowflakeOptions from a protobuf representation of a snowflake option.

        Args:
            snowflake_options_proto: A protobuf representation of a DataSource

        Returns:
            A SnowflakeOptions object based on the snowflake_options protobuf.
        """
        atscale_options: AtScaleOptions = cls(
            features=atscale_options_proto.features.split(", "),
            table_ref=atscale_options_proto.table_ref,
        )

        return atscale_options

    def to_proto(self) -> DataSourceProto.AtScaleOptions:
        """
        Converts an AtScaleOptions object to its protobuf representation.

        Returns:
            A AtScaleOptionsProto protobuf.
        """
        atscale_options_proto = DataSourceProto.AtScaleOptions(
            features=", ".join(self.features), table_ref=self.table_ref
        )

        return atscale_options_proto


class SavedDatasetAtScaleStorage(SavedDatasetStorage):
    _proto_attr_name = "atscale_storage"

    atscale_options: AtScaleOptions

    def __init__(self, table_ref: str, features: list[str]):
        self.atscale_options = AtScaleOptions(features=features)
        self.table_ref = table_ref

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        options: AtScaleOptions = AtScaleOptions.from_proto(
            storage_proto.atscale_storage
        )
        return SavedDatasetAtScaleStorage(
            table_ref=options.table_ref, features=options.features,
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(atscale_storage=self.atscale_options.to_proto())

    def to_data_source(self) -> DataSource:
        return AtScaleSource(features=self.atscale_options.features)
        # todo: use table_ref to create features?
