#publishing_manager

import logging
from sqlalchemy.exc import SQLAlchemyError
from modules.infrastructure.other_ops.file_operations import read_yaml_file
from modules.data_management.sql_utils.sql_ops import execute_sql_operations

logger = logging.getLogger(__name__)

class OutputTable:
    """
    Class to manage output tables for publishing.

    Attributes:
        db_engine (Engine): SQLAlchemy database engine.
        table_name (str): Name of the table.
        publish_as_type_dict (dict): Dictionary specifying the publish type.
        build_table_config (dict): Configuration for building the table.
    """
    def __init__(self, db_engine, table_name, publish_as_type_dict, build_table_config):
        self.db_engine = db_engine
        self.table_name = table_name
        self.publish_as_type = publish_as_type_dict
        self.build_table_config = build_table_config

    def build_table(self):
        """
        Builds the table based on the build_table_config.

        This function executes the SQL operations specified in the build_table_config.
        """
        logger.debug(f"Building table {self.table_name}")
        try:
            # Execute the SQL operations specified in the build_table_config
            if execute_sql_operations(self.db_engine, self.build_table_config):
                logger.debug(f"Table {self.table_name} built successfully.")
            else:
                logger.error(f"Failed to build table {self.table_name}.")
        except Exception as e:
            logger.error(f"Error building table {self.table_name}: {e}")
            raise

    def publish_table(self):
        """
        Publishes the table.

        This function publishes the table to the types specified in the publish_as config.
        """
        logger.debug(f"Publishing table {self.table_name}")
        try:
            # Execute the SQL operations specified in the build_table_config
            if execute_sql_operations(self.db_engine, self.publish_as_type):
                logger.info(f"Table {self.table_name} published successfully.")
            else:
                logger.error(f"Failed to publish table {self.table_name}.")
        except Exception as e:
            logger.error(f"Error publishing table {self.table_name}: {e}")
            raise

class PublishingManager:
    """
    Class to manage the publishing of tables.

    Attributes:
        publishing_config_path (str): Path to the publishing configuration YAML file.
        db_engine (Engine): SQLAlchemy database engine.
        tables (dict): Dictionary of OutputTable objects.
    """
    def __init__(self, publishing_config_path, db_engine):
        self.publishing_config_path = publishing_config_path
        self.db_engine = db_engine
        self.tables = {}
        self._load_config()
        self._initialize_publish_tables()

    def _load_config(self):
        """
        Loads the publishing configuration from the YAML file.
        """
        logger.debug(f"Loading publishing configuration from {self.publishing_config_path}")
        try:
            config = read_yaml_file(self.publishing_config_path)
            self.publishing_config = config.get('publish_tables_configs', {})
            logger.info("Publishing configuration loaded successfully.")
        except Exception as e:
            logger.error(f"Error loading publishing configuration: {e}")
            raise

    def _initialize_publish_tables(self):
        """
        Initializes OutputTable objects from the publishing configuration and stores them in a dictionary.
        """
        logger.debug("Initializing output tables from publishing configuration")
        try:
            for table_name, table_config in self.publishing_config.items():
                publish_as_type_dict = table_config.get('publish_as', {})
                build_table_config = table_config.get('build_table_config', {})
                output_table = OutputTable(
                    db_engine=self.db_engine,
                    table_name=table_name,
                    publish_as_type_dict=publish_as_type_dict,
                    build_table_config=build_table_config
                )
                self.tables[table_name] = output_table
                logger.info(f"Output table {table_name} initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing output tables: {e}")
            raise

    def build_publish_tables(self, table_names=['build_all']):
        """
        Builds the specified tables by calling the build_table method for each OutputTable object in self.tables.

        Args:
            table_names (list): List of table names to build. Defaults to ['build_all'].
        """
        logger.debug("Building output tables")
        try:
            if 'build_all' in table_names:
                table_names = list(self.tables.keys())

            for table_name in table_names:
                if table_name in self.tables:
                    self.tables[table_name].build_table()
                else:
                    logger.warning(f"Table {table_name} not found in the initialized tables.")
        except Exception as e:
            logger.error(f"Error building tables: {e}")
            raise

    def publish_tables(self, table_names=['publish_all']):
        """
        Publishes the specified tables.

        Args:
            table_names (list): List of table names to publish. Defaults to ['publish_all'].
        """
        logger.debug("Publishing output tables")
        try:
            if table_names == ['publish_all']:
                table_names = list(self.tables.keys())

            for table_name in table_names:
                if table_name in self.tables:
                    self.tables[table_name].publish_table()
                else:
                    logger.warning(f"Table {table_name} not found in the initialized tables.")
        except Exception as e:
            logger.error(f"Error publishing tables: {e}")
            raise