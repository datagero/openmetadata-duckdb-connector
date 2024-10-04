#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
Custom Database Service Extracting metadata from a DuckDB database
"""
import duckdb
from pydantic import BaseModel, ValidationError, validator
from pathlib import Path
from typing import Iterable, Optional, List, Dict, Any

from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.entity.data.table import Column
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import (
    CustomDatabaseConnection,
)
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.api.common import Entity
from metadata.ingestion.api.models import Either
from metadata.generated.schema.entity.services.ingestionPipelines.status import StackTraceError
from metadata.ingestion.api.steps import Source
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.parsers.protobuf_parser import ProtobufParser, ProtobufParserConfig
from metadata.utils.logger import ingestion_logger

import logging

logging.basicConfig(level=logging.INFO)
logger = ingestion_logger()

class InvalidDuckDBConnectorException(Exception):
    """
    Sample data is not valid to be ingested
    """


class DuckDBModel(BaseModel):
    database_name: Optional[str]
    database_schema_name: Optional[str]
    table_name: Optional[str]
    column_names: Optional[List[str]]
    column_types: Optional[List[str]]


class DuckDBConnector(Source):
    """
    Custom connector to ingest Database metadata.

    We'll suppose that we can read metadata from a DuckDB database
    with a custom database name from a business_unit connection option.
    """

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        logger.debug("Initializing DuckDBConnector...")

        self.config = config
        logger.debug("Configuration loaded: %s", config)

        self.metadata = metadata
        logger.debug("OpenMetadata initialized.")

        try:
            self.service_connection = config.serviceConnection.root.config
            logger.debug("Service connection established: %s", self.service_connection)
        except AttributeError as e:
            logger.error("Failed to access service connection: %s", e)
            raise

        # logger.info("OpenMetadata attributes: %s", dir(metadata_config))
        # self.metadata_config = metadata_config
        # self.metadata = OpenMetadata(self.metadata_config)


        # Connection params
        try:
            self.database_name: str = self.service_connection.connectionOptions.root.get("database_name")
            logger.debug("Database name: %s", self.database_name)
        except Exception as e:
            logger.error("Error fetching database name: %s", e)
            raise InvalidDuckDBConnectorException("Missing database_name connection option")

        # Additional logging for other attributes
        try:
            self.database_schema_list = (
                self.service_connection.connectionOptions.root.get("database_schema_list")
                .replace(" ", "").split(",")
            )
            logger.debug("Database schema list: %s", self.database_schema_list)
        except Exception as e:
            logger.error("Error fetching database schema list: %s", e)  # Checkpoint 9
            raise InvalidDuckDBConnectorException("Missing database_schema_list connection option")

        try:
            self.database_file_path = self.service_connection.connectionOptions.root.get("database_file_path")
            logger.debug("Database file path: %s", self.database_file_path)
        except Exception as e:
            logger.error("Error fetching database file path: %s", e)
            raise InvalidDuckDBConnectorException("Missing database_file_path connection option")

        self.data: Optional[List[{str, List[DuckDBModel]}]] = []
        super().__init__() # Important to initialise Source object
        logger.info("DuckDBConnector initialization complete.")

    @classmethod
    def create(
        cls, config_dict: dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None
    ) -> "DuckDBConnector":
        logger.debug("Creating DuckDBConnector...")
        try:
            config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
            logger.debug("WorkflowSource parsed successfully.")

            connection: CustomDatabaseConnection = config.serviceConnection.root.config
            logger.debug(f"Connection object created: {connection}")

            if not isinstance(connection, CustomDatabaseConnection):
                raise Exception(
                    f"Expected CustomDatabaseConnection, but got {connection}"
                )
            return cls(config, metadata)
        except Exception as e:
            logger.error(f"Error during connector creation: {e}")
            raise

    def prepare(self):
        logger.debug("Preparing the connector...")
        try:
            # Validate that the db file exists
            source_database_file_path = Path(self.database_file_path)
            if not source_database_file_path.exists():
                logger.error("Source Database file path does not exist.")
                raise InvalidDuckDBConnectorException("Source Database file path does not exist")
            
            logger.debug(f"Database file path exists: {source_database_file_path}")

            # Use a database file (shared between processes)
            conn = duckdb.connect(database=self.database_file_path, read_only=True)
            logger.debug("DuckDB connection established.")

            for schema in self.database_schema_list:
                logger.debug(f"Crawling schema: {schema}")
                self.crawl_schema_table_metadata(conn=conn, database_name=self.database_name, database_schema_name=schema)

            conn.close()
            logger.debug("DuckDB connection closed.")
        except Exception as exc:
            logger.error(f"Unknown error during preparation: {exc}")
            conn.close()
            raise

    def crawl_schema_table_metadata(self, conn, database_name, database_schema_name):
        try:
            logger.info(f"Start crawling the schema: {database_name}.{database_schema_name}")

            sql_get_tables = f"SELECT DISTINCT table_name FROM duckdb_tables() WHERE database_name = '{database_name}' and schema_name = '{database_schema_name}'"
            logger.debug(f"SQL to get tables: {sql_get_tables}")
            table_list = conn.sql(sql_get_tables).fetchall()
            logger.info(f"Tables found: {table_list}")

            table_models = []
            for table in table_list:
                logger.debug(f"Processing table: {table[0]}")
                sql_get_columns = f"SELECT DISTINCT column_name, data_type FROM duckdb_columns() WHERE schema_name = '{database_schema_name}' and table_name = '{table[0]}'"
                logger.debug(f"SQL to get columns: {sql_get_columns}")
                column_list = conn.sql(sql_get_columns).fetchall()
                logger.info(f"Columns found: {column_list}")

                table_model = DuckDBModel(
                    database_name=database_name,
                    database_schema_name=database_schema_name,
                    table_name=table[0],
                    column_names=[column[0] for column in column_list],
                    column_types=[self.convert_data_type(column[1]) for column in column_list]
                )
                table_models.append(table_model)

            self.data.append({"database_schema_name": database_schema_name, "tables": table_models})
            logger.info(f"Finished crawling schema: {database_schema_name}")
        except Exception as e:
            logger.error(f"Error during schema crawling: {e}")
            raise

    def convert_data_type(self, input_data_type):
        # logger.debug(f"Converting data type: {input_data_type}")
        output_data_type = input_data_type

        if input_data_type == "INTEGER":
            output_data_type = "INT"
        elif input_data_type.startswith("DECIMAL"):
            output_data_type = "DECIMAL"
        elif input_data_type.startswith("TIMESTAMP"):
            output_data_type = "TIMESTAMP"

        # logger.debug(f"Converted data type: {output_data_type}")
        return output_data_type

    def yield_create_request_database_service(self):
        logger.info("Yielding database service creation request.")

        service_request = self.metadata.get_create_service_from_source(
            entity=DatabaseService, config=self.config
        )
        yield Either(right=service_request)
        logger.debug("Got database service creation request.")


    def yield_create_request_database(self):
        logger.info("Fetching the service entity for database creation.")
        service_entity: DatabaseService = self.metadata.get_by_name(
            entity=DatabaseService, fqn=self.config.serviceName
        )
        logger.debug(f"Service entity fetched: {service_entity}")

        yield Either(
            right=CreateDatabaseRequest(
                name=self.database_name,
                service=service_entity.fullyQualifiedName,
            )
        )
        logger.debug("Yielded CreateDatabaseRequest.")

    def yield_create_request_schema(self):
        logger.info("Fetching the database entity for schema creation.")
        database_entity: Database = self.metadata.get_by_name(
            entity=Database, fqn=f"{self.config.serviceName}.{self.database_name}"
        )
        logger.debug(f"Database entity fetched: {database_entity}")

        for schema_name in self.database_schema_list:
            logger.debug(f"Yielding CreateDatabaseSchemaRequest for schema: {schema_name}")
            yield Either(
                right=CreateDatabaseSchemaRequest(
                    name=schema_name,
                    database=database_entity.fullyQualifiedName,
                )
            )

    def yield_data(self):
        logger.info("Iterating over data to create tables.")
        for row in self.data:
            logger.debug(f"Processing schema: {row['database_schema_name']}")
            database_schema: DatabaseSchema = self.metadata.get_by_name(
                entity=DatabaseSchema,
                fqn=f"{self.config.serviceName}.{self.database_name}.{row['database_schema_name']}",
            )
            logger.debug(f"Database schema entity fetched: {database_schema}")

            for table_row in row["tables"]:
                logger.debug(f"Yielding CreateTableRequest for table: {table_row.table_name}")
                yield Either(
                    right=CreateTableRequest(
                        name=table_row.table_name,
                        databaseSchema=database_schema.fullyQualifiedName,
                        columns=[
                            Column(
                                name=model_col[0],
                                dataType=model_col[1],
                                dataLength=-1
                            )
                            for model_col in zip(table_row.column_names, table_row.column_types)
                        ],
                    )
                )

    def _iter(self) -> Iterable[Entity]:
        logger.debug("Starting the iteration over database entities.")
        yield from self.yield_create_request_database_service()
        yield from self.yield_create_request_database()
        yield from self.yield_create_request_schema()
        yield from self.yield_data()
        logger.debug("Finished iterating over database entities.")

    def test_connection(self) -> None:
        pass

    def close(self):
        pass