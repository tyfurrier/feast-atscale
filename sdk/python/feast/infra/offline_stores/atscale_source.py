from typing import Callable, Dict, Iterable, Optional, Tuple

import pandas as pd
from feast import type_map
from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.value_type import ValueType

from sdk.python.feast.infra.utils.atscale_utils import get_atscale_api
from atscale.atscale import AtScale


class AtScaleSource(DataSource): #todo allow for limiting to certain features, hierarchies,
    def __init__(
        self,
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
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )

        self.features = features

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
    def from_proto(data_source: DataSourceProto) -> AtScaleSource:
        """
        Creates an AtScaleSource from protobuf representation of one

        Args:
            data_source: A protobuf representation of an AtScaleSource

        Returns:
            An AtScaleSource object based on the data_source
        """
        return AtScaleSource(
            features=list(data_source.features),
            event_timestamp_column=data_source.event_timestamp_column,
            field_mapping=dict(data_source.field_mapping),
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column
        )

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.BATCH_ATSCALE,
            field_mapping=self.field_mapping,
            features=self.features
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        """Checks that each feature can be individually queried, does not check if timestamp exists or if one
        can be inferred"""
        #todo: check full query or timestamp inference upon timestamp being None
        self.get_table_column_names_and_types(config)

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        """
        Returns the callable method that returns Feast type given the raw column type.
        """
        return {
        "ValueType.STRING": ValueType.STRING,
        "ValueType.DOUBLE": ValueType.DOUBLE,
        "ValueType.INT64": ValueType.INT64,
        "ValueType.INT32": ValueType.INT32,
        "ValueType.UNIX_TIMESTAMP": ValueType.UNIX_TIMESTAMP,
        "ValueType.STRING": ValueType.STRING,
        "ValueType.BYTES": ValueType.BYTES,
        "ValueType.UNKNOWN": ValueType.UNKNOWN,
    }

    def get_table_column_names_and_types(
            self, config: RepoConfig
    ) -> Iterable[Tuple[str, ValueType]]:
        """
        Returns the list of column names and feast column types.

        Args:
            config: Configuration object used to configure a feature store.
        """
        atproject: AtScale = get_atscale_api(config)

        description = []
        if self.features is None:
            features = atproject.list_all_features()
            for feat in features:
                description.append((feat, atproject.get_feast_value_type(feature=feat)))
        else:
            for feat in self.features:
                description.append((feat, atproject.get_feast_value_type(feature=feat)))
        return description


    @property
    def features(self) -> list[str]:
        return self._features

    @property

    @property
    def event_timestamp_column(self) -> str:
        return self._event_timestamp_column



