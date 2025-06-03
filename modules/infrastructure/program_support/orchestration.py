import logging
from typing import Any, Dict, List, Tuple
from sqlalchemy.engine import Engine

from modules.infrastructure.program_support.logger_config import configure_logging, LOG_DIVISION
from modules.infrastructure.program_support.settings_config import SettingsManager
from modules.infrastructure.program_support.startup_config import Startup
from modules.data_management.sql_utils.sql_ops import create_engine_with_extensions
from modules.data_management.data_managers.data_source_manager import DataSourceManager
from modules.data_management.data_managers.data_processing_manager import DataProcessingManager
from modules.data_management.data_managers.intersection_tables_manager import IntersectionTablesManager
from modules.data_management.data_managers.publishing_manager import PublishingManager

logger = logging.getLogger(__name__)

def initialize_logger(log_level: int, log_file: str) -> None:
    """
    Initialize the logger.

    Args:
        log_level: Logging level (e.g., logging.DEBUG).
        log_file: Path to the log file.

    Raises:
        Exception: If logger initialization fails.
    """
    try:
        configure_logging(log_level=log_level, log_file=log_file)
        logger.debug("Logger initialized successfully")
    except Exception:
        print(f"CRITICAL: Logger initialization failed\n")
        raise

def load_settings(basic_settings_file: str, advanced_settings_file: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Load and return settings from YAML files.

    Args:
        basic_settings_file: Path to the basic settings YAML.
        advanced_settings_file: Path to the advanced settings YAML.

    Returns:
        Tuple of (basic_settings, advanced_settings) dictionaries.

    Raises:
        Exception: If loading settings fails.
    """
    logger.debug("Attempting to load settings...")
    try:
        settings_manager = SettingsManager(
            basic_settings_file=basic_settings_file,
            advanced_settings_file=advanced_settings_file
        )
        logger.debug("Settings loaded successfully")
        return settings_manager.basic_settings, settings_manager.advanced_settings
    except Exception:
        logger.critical(f"CRITICAL: Load settings failed\n")
        raise

def run_startup_tasks(advanced_settings: Dict[str, Any]) -> None:
    """
    Run startup tasks such as displaying program info and instructions.

    Args:
        advanced_settings: Advanced settings dictionary.

    Raises:
        Exception: If startup tasks fail.
    """
    logger.debug("Attempting to run startup tasks...")
    try:
        startup_configurator = Startup(
            program_info_file=advanced_settings['program_info_file'],
            ascii_art_file=advanced_settings['epa_ascii_art_file'],
            instructions_file=advanced_settings['instructions_file']
        )
        startup_configurator.display_startup_info()
        logger.debug("Startup tasks completed successfully")
        logger.info(LOG_DIVISION)
    except Exception as e:
        logger.critical(f"Failed to run one or more critical startup tasks; ending program\n {e}")
        raise

def connect_to_database(database_url: str) -> Engine:
    """
    Create and return a database connection.

    Args:
        database_url: SQLAlchemy database URL.

    Returns:
        SQLAlchemy Engine instance.

    Raises:
        Exception: If connection fails.
    """
    logger.info("Attempting to connect to database...\n|")
    try:
        db_engine = create_engine_with_extensions(database_url)
        logger.info("Database connection successful")
        logger.info(LOG_DIVISION)
        return db_engine
    except Exception as e:
        logger.critical(f"Could not connect to database; ending program\n {e}")
        raise

def collect_primary_data(
    source_data_path: str,
    source_data_config: str,
    db_engine: Engine,
    sources_to_collect: List[str]
) -> DataSourceManager:
    """
    Collect primary data sources.

    Args:
        source_data_path: Path to source data folder.
        source_data_config: Path to source data config YAML.
        db_engine: SQLAlchemy Engine.
        sources_to_collect: List of source names to collect.

    Returns:
        DataSourceManager instance.

    Raises:
        Exception: If data collection fails.
    """
    logger.info(f"Attempting to collect primary data sources...\n|")
    try:
        data_source_manager = DataSourceManager(
            data_sources_folder=source_data_path,
            source_data_config_path=source_data_config,
            db_engine=db_engine
        )
        data_source_manager.collect_data_sources(sources_to_collect)
        logger.info(f"Primary data collection complete")
        logger.info(LOG_DIVISION)
        return data_source_manager
    except Exception as e:
        logger.critical(f"Error collecting primary data; ending program\n {e}")
        raise

def prepare_data(
    prepared_data_config: str,
    db_engine: Engine,
    data_to_prepare: List[str]
) -> DataProcessingManager:
    """
    Prepare primary data sources for further processing.

    Args:
        prepared_data_config: Path to prepared data config YAML.
        db_engine: SQLAlchemy Engine.
        data_to_prepare: List of data names to prepare.

    Returns:
        DataProcessingManager instance.

    Raises:
        Exception: If data preparation fails.
    """
    logger.info(f"Attempting prepare primary data sources for further processing...\n")
    try:
        data_processing_manager = DataProcessingManager(
            prepared_data_config_path=prepared_data_config,
            db_engine=db_engine
        )
        data_processing_manager.prepare_data(data_to_prepare)
        logger.info(f"Data preparing complete")
        logger.info(LOG_DIVISION)
        return data_processing_manager
    except Exception as e:
        logger.critical(f"Error preparing data; ending program\n {e}")
        raise

def intersect_data(
    intersection_tables_config_path: str,
    intersection_col_names: Dict[str, str],
    db_engine: Engine,
    intersection_tables_settings: Dict[str, Any]
) -> IntersectionTablesManager:
    """
    Build/update intersection tables and run intersections for specified tables and hazards.

    Args:
        intersection_tables_config_path: Path to intersection tables config YAML.
        intersection_col_names: Mapping of intersection table column names.
        db_engine: SQLAlchemy Engine.
        intersection_tables_settings: Dict mapping table names to settings.

    Returns:
        IntersectionTablesManager instance.

    Raises:
        Exception: If intersection processing fails.
    """
    logger.info("Attempting to build/update and run intersections for specified tables and hazards...")
    try:
        intersection_tables_manager = IntersectionTablesManager(
            intersection_tables_config_path=intersection_tables_config_path,
            intersection_col_names=intersection_col_names,
            db_engine=db_engine
        )
        tables_to_update = [
            table_name
            for table_name, table_settings in intersection_tables_settings.items()
            if table_settings.get('update_source', False)
        ]
        if tables_to_update:
            intersection_tables_manager.update_sources(table_names=tables_to_update)
        for table_name, table_settings in intersection_tables_settings.items():
            hazards = table_settings.get('hazards', [])
            intersection_tables_manager.run_intersections(
                table_names=[table_name],
                hazards=hazards
            )
        logger.info("Intersection processing complete")
        logger.info(LOG_DIVISION)
        return intersection_tables_manager
    except Exception as e:
        logger.critical(f"Error running intersections; ending program\n {e}")
        raise

def build_and_publish_tables(
    publishing_config_path: str,
    db_engine: Engine,
    tables_to_publish_settings: Dict[str, Any]
) -> PublishingManager:
    """
    Build and publish tables as specified in the settings.

    Args:
        publishing_config_path: Path to publishing config YAML.
        db_engine: SQLAlchemy Engine.
        tables_to_publish_settings: Dict mapping table names to settings.

    Returns:
        PublishingManager instance.

    Raises:
        Exception: If building or publishing fails.
    """
    logger.info("Attempting to build and publish tables as specified in settings...")
    try:
        publishing_manager = PublishingManager(
            publishing_config_path=publishing_config_path,
            db_engine=db_engine
        )
        tables_to_rebuild = [
            table_name
            for table_name, table_settings in tables_to_publish_settings.items()
            if table_settings.get('rebuild', False)
        ]
        if tables_to_rebuild:
            publishing_manager.build_publish_tables(table_names=tables_to_rebuild)
        for table_name in tables_to_publish_settings.keys():
            publishing_manager.publish_tables(table_names=[table_name])
        logger.info("Publishing processing complete")
        logger.info(LOG_DIVISION)
        return publishing_manager
    except Exception as e:
        logger.critical(f"Error building or publishing tables; ending program\n {e}")
        raise