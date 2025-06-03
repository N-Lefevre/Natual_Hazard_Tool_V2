"""
data_processing_manager.py

Manages the processing of primary data tables by executing SQL actions defined in configuration files.
"""

import logging
from typing import Dict, Any, Optional, List
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from modules.infrastructure.other_ops.file_operations import read_yaml_file

logger = logging.getLogger(__name__)

class PreparedData:
    """
    Holds information about each prepared data table and executes SQL actions.
    """
    def __init__(self, table_name: str, sql_actions: str, db_engine: Engine) -> None:
        """
        Initialize the PreparedData object.

        Args:
            table_name (str): Name of the prepared data table.
            sql_actions (str): SQL actions to be executed for this table.
            db_engine (Engine): SQLAlchemy database engine.
        """
        self.table_name: str = table_name
        self.sql_actions: str = sql_actions
        self.db_engine: Engine = db_engine

    def execute_sql(self) -> bool:
        """
        Execute the SQL actions using the provided database engine.

        Returns:
            bool: True if all SQL actions executed successfully, False otherwise.
        """
        try:
            with self.db_engine.connect() as connection:
                trans = connection.begin()
                try:
                    for sql_action in self.sql_actions.split('\n'):
                        if sql_action.strip():
                            logger.debug(f"Executing SQL for {self.table_name}: {sql_action.strip()}")
                            connection.execute(text(sql_action.strip()))
                    trans.commit()
                    return True
                except SQLAlchemyError as e:
                    trans.rollback()
                    logger.error(f"Error executing SQL for {self.table_name}: {e}")
        except SQLAlchemyError as e:
            logger.error(f"Error connecting to the database for {self.table_name}: {e}")
        return False

class DataProcessingManager:
    """
    Manages the processing of primary data and prepares it for further processing.
    """
    def __init__(self, prepared_data_config_path: str, db_engine: Engine) -> None:
        """
        Initialize the DataProcessingManager object.

        Args:
            prepared_data_config_path (str): Path to the YAML configuration file for processed data.
            db_engine (Engine): SQLAlchemy database engine.
        """
        self.prepared_data_config_path: str = prepared_data_config_path
        self.db_engine: Engine = db_engine
        self.prepared_data_config: Dict[str, Any] = self.load_prepared_data_configs()
        self.processed_sources: Dict[str, PreparedData] = self.create_processed_sources()

    def load_prepared_data_configs(self) -> Dict[str, Any]:
        logger.debug(f"Loading processing configurations from {self.prepared_data_config_path}")
        config = read_yaml_file(self.prepared_data_config_path)
        return config

    def create_processed_sources(self) -> Dict[str, PreparedData]:
        processed_sources: Dict[str, PreparedData] = {}
        logger.info("Creating processed sources")
        for table_name, sql_actions in self.prepared_data_config.items():
            try:
                processed_sources[table_name] = PreparedData(table_name, sql_actions, self.db_engine)
                logger.debug(f"Created PreparedData object for {table_name}")
            except Exception as e:
                logger.error(f"Failed to create PreparedData object for {table_name}: {e}")
        return processed_sources

    def _determine_prepared_data_names(self, processed_data_names: Optional[List[str]]) -> List[str]:
        """
        Determine which processed data should be prepared.

        Args:
            processed_data_names (Optional[List[str]]): List of names to prepare, or None.

        Returns:
            List[str]: List of processed data names to prepare.
        """
        if processed_data_names is None:
            logger.info("No processed data names specified. No data will be prepared.")
            return []
        if 'prepare_all' in processed_data_names:
            return list(self.processed_sources.keys())
        return [name for name in processed_data_names if name in self.processed_sources]

    def prepare_data(self, processed_data_names: Optional[List[str]] = None) -> None:
        """
        Execute the SQL actions for the specified processed data names.

        Args:
            processed_data_names (Optional[List[str]]): Names of the processed data to run.
                If None, no data will be prepared.
                If 'prepare_all' is in the list, all PreparedData objects will be prepared.
                Otherwise, only the specified names will be prepared.
        """
        names_to_prepare = self._determine_prepared_data_names(processed_data_names)
        logger.info(f"Preparing the following data: {names_to_prepare}")

        for table_name in names_to_prepare:
            prepared_data = self.processed_sources[table_name]
            success = prepared_data.execute_sql()
            if success:
                logger.info(f"Prepared data successfully: {table_name}")
            else:
                logger.error(f"Failed to prepare data: {table_name}")

        # Log any names requested but not found
        if processed_data_names is not None and 'prepare_all' not in processed_data_names:
            not_found = [name for name in processed_data_names if name not in self.processed_sources]
            for name in not_found:
                logger.warning(f"Processed data table name {name} not found in processed sources configuration")