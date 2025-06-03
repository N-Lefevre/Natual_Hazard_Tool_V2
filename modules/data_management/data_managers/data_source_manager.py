# import logging
# import os
# import importlib
# import geopandas as gpd

# from modules.infrastructure.other_ops.file_operations import read_yaml_file
# from modules.data_management.sql_utils.sql_ops import drop_and_rebuild_table

# logger = logging.getLogger(__name__)

# class DataSource:
#     """
#     Class to represent a data source and manage its collection method.

#     Attributes:
#         name (str): Name of the data source.
#         source_config (dict): Configuration dictionary for the data source.
#         db_engine (Engine): SQLAlchemy engine connected to the database.
#         collection_method (object): Instance of the collection method class.
#         table_name (str): Name of the database table for the data source.
#         table_columns (list): List of columns for the database table.
#     """
#     def __init__(self, name, source_config, db_engine):
#         """
#         Initialize the DataSource with a configuration and name.

#         Args:
#             name (str): Name of the data source.
#             source_config (dict): Configuration dictionary for the data source.
#             db_engine (Engine): SQLAlchemy engine connected to the database.

#         Raises:
#             ValueError: If the source_config is missing required keys.
#         """
#         self.name = name
#         self.source_config = source_config
#         self.db_engine = db_engine

#         self._validate_source_config()

#         self.collection_method = self._initialize_collection_method()
#         self.table_name = self.source_config['table']['table_name']
#         self.table_columns = self.source_config['table']['table_columns']

#         logger.debug(f"DataSource '{self.name}' initialized successfully.")

#     def _validate_source_config(self):
#         """
#         Validate the source configuration to ensure required keys are present.

#         Raises:
#             ValueError: If required keys are missing in the source_config.
#         """
#         required_keys = ['method', 'method_configs', 'table']
#         table_required_keys = ['table_name', 'table_columns']

#         for key in required_keys:
#             if key not in self.source_config:
#                 raise ValueError(f"Missing required key '{key}' in source_config for data source '{self.name}'.")

#         for key in table_required_keys:
#             if key not in self.source_config['table']:
#                 raise ValueError(f"Missing required key '{key}' in table configuration for data source '{self.name}'.")

#     def _initialize_collection_method(self):
#         """
#         Initialize the collection method for the data source based on its configuration.

#         Returns:
#             object: An instance of the collection method class, or None if initialization fails.

#         Raises:
#             ImportError: If the specified collection method module cannot be imported.
#             AttributeError: If the specified collection method class is not found in the module.
#         """
#         method_name = self.source_config['method']

#         if not method_name:
#             logger.error(f"Collection method not specified for data source '{self.name}'.")
#             return None

#         try:
#             module = importlib.import_module(f'modules.data_management.collection_methods.{method_name}')
#             method_class = getattr(module, method_name)
#             logger.debug(f"Collection method '{method_name}' initialized successfully for data source '{self.name}'.")
#             return method_class
#         except ImportError as e:
#             logger.error(f"Failed to import collection method module '{method_name}' for data source '{self.name}': {e}")
#         except AttributeError as e:
#             logger.error(f"Collection method class '{method_name}' not found in module for data source '{self.name}': {e}")
#         except Exception as e:
#             logger.error(f"Unexpected error initializing collection method '{method_name}' for data source '{self.name}': {e}")

#         return None

#     def update_table(self):
#         """
#         Build or update the table for the data source based on its configuration.

#         Logs an error if the table update fails.
#         """
#         if not self.table_name or not self.table_columns:
#             logger.warning(f"Table name or columns not defined for data source '{self.name}'. Skipping table update.")
#             return

#         try:
#             success = drop_and_rebuild_table(self.db_engine, self.table_name, self.table_columns)
#             if success:
#                 logger.debug(f"Table '{self.table_name}' updated successfully for data source '{self.name}'.")
#             else:
#                 logger.error(f"Failed to build or update table '{self.table_name}' for data source '{self.name}'.")
#         except Exception as e:
#             logger.error(f"Unexpected error updating table '{self.table_name}' for data source '{self.name}': {e}")

# class DataSourceManager:
#     """
#     Class to determine data sources to collect, retrieve their configurations, and perform the collection from their sources.

#     Attributes:
#         data_sources_folder (str): Path to the folder for storing data if necessary.
#         source_data_config_path (str): Path to the folder containing source data configurations.
#         db_engine (Engine): SQLAlchemy engine connected to the database.
#         sources_configs (dict): Dictionary of DataSource configurations loaded from the YAML file.
#         data_sources (dict): Dictionary of DataSource instances created from the configurations.
#         sources_to_collect_names (list): List of data source names to be collected.
#     """
#     def __init__(self, data_sources_folder, source_data_config_path, db_engine):
#         """
#         Initialize the DataSourceManager with folder paths.

#         Args:
#             data_sources_folder (str): Path to the folder for storing data if necessary.
#             source_data_config_path (str): Path to the folder containing source data configurations.
#             db_engine (Engine): SQLAlchemy engine connected to the database.

#         Raises:
#             FileNotFoundError: If the source data configuration file does not exist.
#         """
#         self.data_sources_folder = data_sources_folder
#         self.source_data_config_path = source_data_config_path
#         self.db_engine = db_engine

#         self._validate_config_path()

#         # Load configurations and create data sources
#         self.sources_configs = self.load_source_configs()
#         self.data_sources = self.create_datasources()
#         self.sources_to_collect_names = []

#         logger.debug("DataSourceManager initialization complete.")

#     def _validate_config_path(self):
#         """
#         Validate the source data configuration file path.

#         Raises:
#             FileNotFoundError: If the configuration file does not exist.
#         """
#         if not os.path.isfile(self.source_data_config_path):
#             logger.critical(f"Source data configuration file not found: {self.source_data_config_path}")
#             raise FileNotFoundError(f"Source data configuration file not found: {self.source_data_config_path}")

#     def load_source_configs(self):
#         """
#         Load source configurations from a YAML file.

#         Returns:
#             dict: Dictionary of source configurations.

#         Raises:
#             ValueError: If the configuration file is empty or invalid.
#         """
#         try:
#             source_configs = read_yaml_file(self.source_data_config_path)
#             if not source_configs:
#                 raise ValueError("Source data configuration file is empty or invalid.")
#             logger.debug(f"Source configurations loaded successfully from {self.source_data_config_path}.")
#             return source_configs
#         except Exception as e:
#             logger.error(f"Failed to load source configurations from {self.source_data_config_path}: {e}")
#             raise

#     def create_datasources(self):
#         """
#         Create DataSource instances from the loaded configurations.

#         Returns:
#             dict: Dictionary of DataSource instances.
#         """
#         data_sources = {}
#         for name, config in self.sources_configs.items():
#             try:
#                 data_sources[name] = DataSource(name, config, self.db_engine)
#                 logger.debug(f"DataSource '{name}' created successfully.")
#             except Exception as e:
#                 logger.error(f"Failed to create DataSource '{name}': {e}")
#         return data_sources

#     def determine_collection_sources(self, source_names=None):
#         """
#         Determine which data sources should be collected based on the provided source names.

#         Args:
#             source_names (list): Names of the data sources to collect data from. If None, no sources will be collected.

#         Returns:
#             bool: False if no sources will be collected. True if there are data sources to collect.
#         """
#         try:
#             self.sources_to_collect_names = []

#             if source_names is None:
#                 logger.info("No primary sources specified for collection.")
#                 return False

#             if 'collect_all' in source_names:
#                 self.sources_to_collect_names = list(self.data_sources.keys())
#                 logger.debug("All primary sources will be collected.")
#                 return True

#             for name in source_names:
#                 if name in self.data_sources:
#                     self.sources_to_collect_names.append(name)
#                 else:
#                     logger.warning(f"Source '{name}' was specified for collection, but was not found in data sources config. Cannot attempt to collect.")

#             return bool(self.sources_to_collect_names)
#         except Exception as e:
#             logger.error(f"Failed to determine sources to collect: {e}")
#             return False

#     def collect_data_sources(self, source_names=None):
#         """
#         Collect data from the specified data sources and save it to the database.

#         Args:
#             source_names (list): Names of the data sources to collect data from. If None, no sources will be collected.

#         Returns:
#             list: List of successfully collected data source names.
#         """
#         collected_sources = []
#         try:
#             if self.determine_collection_sources(source_names):
#                 logger.info(f"Collecting primary source(s): {', '.join(self.sources_to_collect_names)}")

#                 for data_source_name in self.sources_to_collect_names:
#                     data_source = self.data_sources[data_source_name]
#                     try:
#                         logger.info(f"Attempting to collect data for: {data_source.name}")
#                         source_collector = data_source.collection_method(data_source, self.db_engine, self.data_sources_folder)
#                         data_source.update_table()
#                         success = source_collector.collect_data()
#                         if success:
#                             logger.info(f"Data collection successful for: {data_source.name}")
#                             collected_sources.append(data_source.name)
#                         else:
#                             logger.error(f"Data collection failed for: {data_source.name}")
#                     except Exception as e:
#                         logger.error(f"Error collecting data for: {data_source.name}: {e}")
#             return collected_sources
#         except Exception as e:
#             logger.error(f"Error collecting data sources: {e}")

"""
data_source_manager.py

Manages data source configurations, instantiates data source objects, and coordinates data collection and table management for each source.
"""

import logging
import os
import importlib
import geopandas as gpd
from typing import Dict, Any, List, Optional
from sqlalchemy.engine import Engine

from modules.infrastructure.other_ops.file_operations import read_yaml_file
from modules.data_management.sql_utils.sql_ops import drop_and_rebuild_table

logger = logging.getLogger(__name__)

class DataSource:
    """
    Represents a data source and manages its collection method and table configuration.

    Attributes:
        name (str): Name of the data source.
        source_config (dict): Configuration dictionary for the data source.
        db_engine (Engine): SQLAlchemy engine connected to the database.
        collection_method (object): Instance of the collection method class.
        table_name (str): Name of the database table for the data source.
        table_columns (dict): Dictionary of columns for the database table.
    """
    def __init__(self, name: str, source_config: Dict[str, Any], db_engine: Engine) -> None:
        """
        Initialize the DataSource with a configuration and name.

        Args:
            name (str): Name of the data source.
            source_config (dict): Configuration dictionary for the data source.
            db_engine (Engine): SQLAlchemy engine connected to the database.

        Raises:
            ValueError: If the source_config is missing required keys.
        """
        self.name: str = name
        self.source_config: Dict[str, Any] = source_config
        self.db_engine: Engine = db_engine

        self._validate_source_config()

        self.collection_method: Optional[Any] = self._initialize_collection_method()
        self.table_name: str = self.source_config['table']['table_name']
        self.table_columns: Dict[str, str] = self.source_config['table']['table_columns']

        logger.debug(f"DataSource '{self.name}' initialized successfully.")

    def _validate_source_config(self) -> None:
        """
        Validate the source configuration to ensure required keys are present.

        Raises:
            ValueError: If required keys are missing in the source_config.
        """
        required_keys = ['method', 'method_configs', 'table']
        table_required_keys = ['table_name', 'table_columns']

        for key in required_keys:
            if key not in self.source_config:
                raise ValueError(f"Missing required key '{key}' in source_config for data source '{self.name}'.")

        for key in table_required_keys:
            if key not in self.source_config['table']:
                raise ValueError(f"Missing required key '{key}' in table configuration for data source '{self.name}'.")

    def _initialize_collection_method(self) -> Optional[Any]:
        """
        Initialize the collection method for the data source based on its configuration.

        Returns:
            object: An instance of the collection method class, or None if initialization fails.

        Raises:
            ImportError: If the specified collection method module cannot be imported.
            AttributeError: If the specified collection method class is not found in the module.
        """
        method_name = self.source_config['method']

        if not method_name:
            logger.error(f"Collection method not specified for data source '{self.name}'.")
            return None

        try:
            module = importlib.import_module(f'modules.data_management.collection_methods.{method_name}')
            method_class = getattr(module, method_name)
            logger.debug(f"Collection method '{method_name}' initialized successfully for data source '{self.name}'.")
            return method_class
        except ImportError as e:
            logger.error(f"Failed to import collection method module '{method_name}' for data source '{self.name}': {e}")
        except AttributeError as e:
            logger.error(f"Collection method class '{method_name}' not found in module for data source '{self.name}': {e}")
        except Exception as e:
            logger.error(f"Unexpected error initializing collection method '{method_name}' for data source '{self.name}': {e}")

        return None

    def update_table(self) -> None:
        """
        Build or update the table for the data source based on its configuration.

        Logs an error if the table update fails.
        """
        if not self.table_name or not self.table_columns:
            logger.warning(f"Table name or columns not defined for data source '{self.name}'. Skipping table update.")
            return

        try:
            success = drop_and_rebuild_table(self.db_engine, self.table_name, self.table_columns)
            if success:
                logger.debug(f"Table '{self.table_name}' updated successfully for data source '{self.name}'.")
            else:
                logger.error(f"Failed to build or update table '{self.table_name}' for data source '{self.name}'.")
        except Exception as e:
            logger.error(f"Unexpected error updating table '{self.table_name}' for data source '{self.name}': {e}")

class DataSourceManager:
    """
    Determines data sources to collect, retrieves their configurations, and performs the collection from their sources.

    Attributes:
        data_sources_folder (str): Path to the folder for storing data if necessary.
        source_data_config_path (str): Path to the folder containing source data configurations.
        db_engine (Engine): SQLAlchemy engine connected to the database.
        sources_configs (dict): Dictionary of DataSource configurations loaded from the YAML file.
        data_sources (dict): Dictionary of DataSource instances created from the configurations.
        sources_to_collect_names (list): List of data source names to be collected.
    """
    def __init__(self, data_sources_folder: str, source_data_config_path: str, db_engine: Engine) -> None:
        """
        Initialize the DataSourceManager with folder paths.

        Args:
            data_sources_folder (str): Path to the folder for storing data if necessary.
            source_data_config_path (str): Path to the folder containing source data configurations.
            db_engine (Engine): SQLAlchemy engine connected to the database.

        Raises:
            FileNotFoundError: If the source data configuration file does not exist.
        """
        self.data_sources_folder: str = data_sources_folder
        self.source_data_config_path: str = source_data_config_path
        self.db_engine: Engine = db_engine

        self._validate_config_path()

        # Load configurations and create data sources
        self.sources_configs: Dict[str, Any] = self.load_source_configs()
        self.data_sources: Dict[str, DataSource] = self.create_datasources()
        self.sources_to_collect_names: List[str] = []

        logger.debug("DataSourceManager initialization complete.")

    def _validate_config_path(self) -> None:
        """
        Validate the source data configuration file path.

        Raises:
            FileNotFoundError: If the configuration file does not exist.
        """
        if not os.path.isfile(self.source_data_config_path):
            logger.critical(f"Source data configuration file not found: {self.source_data_config_path}")
            raise FileNotFoundError(f"Source data configuration file not found: {self.source_data_config_path}")

    def load_source_configs(self) -> Dict[str, Any]:
        """
        Load source configurations from a YAML file.

        Returns:
            dict: Dictionary of source configurations.

        Raises:
            ValueError: If the configuration file is empty or invalid.
        """
        try:
            source_configs = read_yaml_file(self.source_data_config_path)
            if not source_configs:
                raise ValueError("Source data configuration file is empty or invalid.")
            logger.debug(f"Source configurations loaded successfully from {self.source_data_config_path}.")
            return source_configs
        except Exception as e:
            logger.error(f"Failed to load source configurations from {self.source_data_config_path}: {e}")
            raise

    def create_datasources(self) -> Dict[str, DataSource]:
        """
        Create DataSource instances from the loaded configurations.

        Returns:
            dict: Dictionary of DataSource instances.
        """
        data_sources: Dict[str, DataSource] = {}
        for name, config in self.sources_configs.items():
            try:
                data_sources[name] = DataSource(name, config, self.db_engine)
                logger.debug(f"DataSource '{name}' created successfully.")
            except Exception as e:
                logger.error(f"Failed to create DataSource '{name}': {e}")
        return data_sources

    def determine_collection_sources(self, source_names: Optional[List[str]] = None) -> bool:
        """
        Determine which data sources should be collected based on the provided source names.

        Args:
            source_names (list, optional): Names of the data sources to collect data from. If None, no sources will be collected.

        Returns:
            bool: False if no sources will be collected. True if there are data sources to collect.
        """
        try:
            self.sources_to_collect_names = []

            if source_names is None:
                logger.info("No primary sources specified for collection.")
                return False

            if 'collect_all' in source_names:
                self.sources_to_collect_names = list(self.data_sources.keys())
                logger.debug("All primary sources will be collected.")
                return True

            for name in source_names:
                if name in self.data_sources:
                    self.sources_to_collect_names.append(name)
                else:
                    logger.warning(f"Source '{name}' was specified for collection, but was not found in data sources config. Cannot attempt to collect.")

            return bool(self.sources_to_collect_names)
        except Exception as e:
            logger.error(f"Failed to determine sources to collect: {e}")
            return False

    def collect_data_sources(self, source_names: Optional[List[str]] = None) -> List[str]:
        """
        Collect data from the specified data sources and save it to the database.

        Args:
            source_names (list, optional): Names of the data sources to collect data from. If None, no sources will be collected.

        Returns:
            list: List of successfully collected data source names.
        """
        collected_sources: List[str] = []
        try:
            if self.determine_collection_sources(source_names):
                logger.info(f"Collecting primary source(s): {', '.join(self.sources_to_collect_names)}")

                for data_source_name in self.sources_to_collect_names:
                    data_source = self.data_sources[data_source_name]
                    try:
                        logger.info(f"Attempting to collect data for: {data_source.name}")
                        source_collector = data_source.collection_method(data_source, self.db_engine, self.data_sources_folder)
                        data_source.update_table()
                        success = source_collector.collect_data()
                        if success:
                            logger.info(f"Data collection successful for: {data_source.name}")
                            collected_sources.append(data_source.name)
                        else:
                            logger.error(f"Data collection failed for: {data_source.name}")
                    except Exception as e:
                        logger.error(f"Error collecting data for: {data_source.name}: {e}")
            return collected_sources
        except Exception as e:
            logger.error(f"Error collecting data sources: {e}")
            return collected_sources