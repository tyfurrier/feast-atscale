import uuid
from typing import Dict

import pandas as pd
import requests
from atscale.atscale import AtScale

from feast import AtScaleSource
from feast.data_source import DataSource
from feast.infra.offline_stores.atscale_offline_store import AtScaleOfflineStoreConfig
from feast.infra.offline_stores.atscale_source import SavedDatasetAtScaleStorage
from feast.infra.utils.atscale_utils import get_atscale_api
from feast.saved_dataset import SavedDatasetStorage
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class AtScaleDataSourceCreator(DataSourceCreator):
    tables: list[str] = []

    def __init__(self, project_name: str):
        super().__init__()
        self.project_name = project_name
        self.offline_store_config = AtScaleOfflineStoreConfig(
            type="atscale.offline",
            server="your server",
            project_id="any project id",
            model_id="any corresponding model id",
            organization="ur org",
            username="ur username",
            password="ur password",
            db_connection_id="ur connection id/group",
            db_type="the db type",
            #add params for ur db_type
        )
        self.atproject = get_atscale_api(self.offline_store_config)

        driver_proj: dict = {
            "id": "80f88602-38c0-4523-7d6a-c9c97543d1b7",
            "name": project_name,
            "version": 39,
            "annotations": {
                "annotation": [
                    {"name": "migrationVersion", "value": "2021.4.0.4227"},
                    {"name": "version", "value": "2"},
                    {
                        "name": "engineId",
                        "value": "6fa6e60a-1a80-4cd3-79e4-a02b81e9ceaa",
                    },
                    {"name": "versionTag"},
                    {
                        "name": "publishDate",
                        "value": "Published on Apr 5, 2022 at 11:09pm (UTC)",
                    },
                ]
            },
            "properties": {
                "caption": "feast-starter",
                "visible": True,
                "aggressive_aggregate_promotion": False,
                "aggregate-prediction": {"speculative-aggregates": False},
            },
            "attributes": {
                "attribute-key": [
                    {
                        "id": "bd2a83e7-f127-4e69-8b04-385e8c5254d1",
                        "properties": {"visible": True, "columns": 1},
                    }
                ],
                "keyed-attribute": [
                    {
                        "id": "9b26c3be-1b08-4ace-66fa-09dae10b6e89",
                        "key-ref": "bd2a83e7-f127-4e69-8b04-385e8c5254d1",
                        "name": "Date Level",
                        "properties": {
                            "caption": "Date Level",
                            "visible": True,
                            "type": {"enum": {}},
                        },
                    }
                ],
            },
            "dimensions": {
                "dimension": [
                    {
                        "id": "72ea8f8a-28e2-4719-651a-bf4883d692ab",
                        "name": "Date",
                        "properties": {"visible": True, "dimension-type": "Time"},
                        "hierarchy": [
                            {
                                "id": "e1b100d6-c27a-480f-8aab-59c0de0b344a",
                                "name": "Date Hierarchy",
                                "properties": {
                                    "caption": "Date Hierarchy",
                                    "visible": True,
                                    "folder": "Dimensions",
                                    "filter-empty": "Yes",
                                    "default-member": {"all-member": {}},
                                },
                                "level": [
                                    {
                                        "id": "591fe842-9271-4b4a-9887-4dea6f358d6c",
                                        "primary-attribute": "9b26c3be-1b08-4ace-66fa-09dae10b6e89",
                                        "properties": {
                                            "unique-in-parent": False,
                                            "visible": True,
                                            "level-type": "TimeDays",
                                        },
                                    }
                                ],
                            }
                        ],
                    }
                ]
            },
            "datasets": {
                "data-set": [
                    {
                        "id": "b33cdf57-b72f-4dd8-9fe7-dc82a0d1536a",
                        "name": "starter",
                        "properties": {
                            "allow-aggregates": True,
                            "aggregate-locality": None,
                            "aggregate-destinations": None,
                        },
                        "physical": {
                            "connection": {"id": "Snowflake"},
                            "tables": [
                                {
                                    "database": "SE_DEMO_LIBRARY",
                                    "schema": "DBG_TABLES",
                                    "name": "TEST_VALUETYPEINT64INT64TRUE_6F4B57_1_TEST_VALUETYPEINT64INT64TRUE_6F4B57_1",
                                }
                            ],
                            "immutable": False,
                            "columns": [
                                {
                                    "id": "f5c58854-11ed-487b-874b-c75864c14e17",
                                    "name": "DRIVER_ID",
                                    "type": {
                                        "data-type": "Decimal(38,0)",
                                        "supported": None,
                                    },
                                },
                                {
                                    "id": "80b36b85-82b6-4c37-8fd3-1dfbb86675be",
                                    "name": "VALUE",
                                    "type": {"data-type": "String", "supported": None},
                                },
                                {
                                    "id": "0836f215-379c-4e24-a65e-235579e6d9c3",
                                    "name": "TS_1",
                                    "type": {"data-type": "String", "supported": None},
                                },
                                {
                                    "id": "7f9dfd15-3a9f-45c3-907c-e47478372671",
                                    "name": "CREATED_TS",
                                    "type": {
                                        "data-type": "DateTime",
                                        "supported": None,
                                    },
                                },
                            ],
                        },
                        "logical": {
                            "key-ref": [
                                {
                                    "id": "bd2a83e7-f127-4e69-8b04-385e8c5254d1",
                                    "unique": False,
                                    "complete": "true",
                                    "column": ["TS_1"],
                                }
                            ],
                            "attribute-ref": [
                                {
                                    "id": "9b26c3be-1b08-4ace-66fa-09dae10b6e89",
                                    "complete": "true",
                                    "column": ["TS_1"],
                                }
                            ],
                        },
                    }
                ]
            },
            "calculated-members": {},
            "cubes": {
                "cube": [
                    {
                        "id": "c5548bca-0d22-4d6a-72af-452a25231d96",
                        "name": "drivers",
                        "properties": {"caption": "blank", "visible": False},
                        "attributes": {
                            "attribute": [
                                {
                                    "id": "9cc82677-f40a-47cd-b449-4798c24c61d2",
                                    "name": "conv_rate",
                                    "properties": {
                                        "caption": "Avg Conv Rate",
                                        "visible": True,
                                        "folder": "Features",
                                        "type": {
                                            "measure": {"default-aggregation": "AVG"}
                                        },
                                    },
                                },
                                {
                                    "id": "6f407800-0357-4a01-aa23-0c9ce189e0e0",
                                    "name": "avg_daily_trips",
                                    "properties": {
                                        "caption": "Avg Daily Trips",
                                        "visible": True,
                                        "folder": "Features",
                                        "type": {
                                            "measure": {"default-aggregation": "AVG"}
                                        },
                                    },
                                },
                                {
                                    "id": "5edcf276-51b6-42eb-ab2c-0cb6499fb89b",
                                    "name": "acc_rate",
                                    "properties": {
                                        "caption": "Avg Acc Rate",
                                        "visible": True,
                                        "folder": "Features",
                                        "type": {
                                            "measure": {"default-aggregation": "AVG"}
                                        },
                                    },
                                },
                                {
                                    "id": "08dda43f-83d9-4277-9b0c-80842490694e",
                                    "name": "m_CONV_RATE_sum",
                                    "properties": {
                                        "caption": "Total Conv Rate",
                                        "visible": True,
                                        "folder": "Features",
                                        "type": {
                                            "measure": {"default-aggregation": "SUM"}
                                        },
                                    },
                                },
                                {
                                    "id": "40eac99e-4770-49c1-83f4-13732681df7c",
                                    "name": "m_VALUE_sum",
                                    "properties": {
                                        "caption": "value",
                                        "visible": True,
                                        "folder": "Features",
                                        "type": {
                                            "measure": {"default-aggregation": "SUM"}
                                        },
                                    },
                                },
                            ]
                        },
                        "dimensions": {},
                        "actions": {
                            "properties": {"include-default-drill-through": True}
                        },
                        "data-sets": {
                            "data-set-ref": [
                                {
                                    "uiid": "817e5488-fc5d-411c-4d22-308465533223",
                                    "id": "b33cdf57-b72f-4dd8-9fe7-dc82a0d1536a",
                                    "properties": {
                                        "allow-aggregates": True,
                                        "create-hinted-aggregate": False,
                                        "aggregate-destinations": {
                                            "allow-local": True,
                                            "allow-peer": True,
                                            "allow-preferred": True,
                                        },
                                    },
                                    "logical": {
                                        "key-ref": [
                                            {
                                                "id": "bd2a83e7-f127-4e69-8b04-385e8c5254d1",
                                                "unique": False,
                                                "complete": "false",
                                                "column": ["TS_1"],
                                            }
                                        ],
                                        "attribute-ref": [
                                            {
                                                "id": "40eac99e-4770-49c1-83f4-13732681df7c",
                                                "complete": "true",
                                                "column": ["VALUE"],
                                            }
                                        ],
                                    },
                                }
                            ]
                        },
                        "calculated-members": {},
                        "aggregates": {},
                    }
                ]
            },
        }

        model_id = driver_proj["cubes"]["cube"][0]["id"]
        project_id = self.atproject.create_new_project(json_data=driver_proj)
        db = self.atproject.database

        self.atproject = AtScale(
            server=self.offline_store_config.server,
            organization=self.offline_store_config.organization,
            model_id=model_id,
            project_id=project_id,
            username=self.offline_store_config.username,
            password=self.offline_store_config.password,
        )

        self.atproject.create_db_connection(db=db)
        self.atproject.publish_project()
        self.atproject.refresh_project()

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
        field_mapping: Dict[str, str] = None,
    ) -> DataSource:
        """
        Create a data source based on the dataframe. Implementing this method requires the underlying implementation to
        persist the dataframe in offline store, using the destination string as a way to differentiate multiple
        dataframes and data sources.

        Args:
            df: The dataframe to be used to create the data source.
            destination_name: This str is used by the implementing classes to
                isolate the multiple dataframes from each other.
            event_timestamp_column: Pass through for the underlying data source.
            created_timestamp_column: Pass through for the underlying data source.
            field_mapping: Pass through for the underlying data source.

        Returns:
            A Data source object, pointing to a table or file that is uploaded/persisted for the purpose of the
            test.
        """
        dest_name = self._get_prefixed_table_name(destination_name)

        self.tables.append(dest_name)

        timestamp_to_col: dict[str, str] = {}
        for col in df.columns:
            timestamp_to_col[col] = col
        if field_mapping is not None:
            assert isinstance(field_mapping, dict)
            for (k, v) in field_mapping.items():
                timestamp_to_col[v] = k
        try:
            self.atproject.add_table(
                table_name=dest_name,
                dataframe=df,
                join_features=[],
                join_columns=[],
                if_exists="fail",
            )
        except Exception:
            self.atproject.database.execute_statement(
                f'drop table if exists "{self.atproject.database.fix_table_name(dest_name)}"'
            )
            self.atproject.add_table(
                table_name=dest_name,
                dataframe=df,
                join_features=["Date Level"],
                join_columns=[timestamp_to_col[event_timestamp_column]],
                if_exists="fail",
            )
        self.atproject.refresh_project()

        features = []
        dest_name_fixed = self.atproject.database.fix_table_name(dest_name)
        for feat in df.columns:
            self.atproject.create_aggregate_feature(
                dest_name_fixed,
                self.atproject.database.fix_table_name(feat),
                f"{dest_name}_{feat}",
                "SUM",
                folder=dest_name,
            )
            features.append(feat)

        return AtScaleSource(
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {"ts_1": "ts"},
            features=features,
        )

    def create_offline_store_config(self) -> AtScaleOfflineStoreConfig:
        return self.offline_store_config

    def create_saved_dataset_destination(self) -> SavedDatasetStorage:
        table = self.get_prefixed_table_name(
            f"persisted_{str(uuid.uuid4()).replace('-', '_')}"
        )
        self.tables.append(table)
        return SavedDatasetAtScaleStorage(table_ref=table, features=[])

    def teardown(self):
        for table in self.tables:
            self.atproject.database.execute_statement(f'DROP TABLE IF EXISTS "{table}"')

        # delete project
        url = f"{self.atproject.server}:{self.atproject.design_center_server_port}/api/1.0/org/{self.atproject.organization}/project/{self.atproject.project_id}"
        headers = {
            "Content-type": "application/json",
            "Authorization": f"Bearer {self.atproject.token}",
        }
        requests.delete(f"{url}", headers=headers)

    def _get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}_{suffix}"
