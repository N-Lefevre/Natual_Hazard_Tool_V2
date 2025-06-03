"""
Climate Screening Tool - Main Program
=====================================
Entry point for the Natural Hazard Screening Tool.
"""

import sys
import os
import logging

from modules.infrastructure.program_support.orchestration import (
    initialize_logger,
    load_settings,
    run_startup_tasks,
    connect_to_database,
    collect_primary_data,
    prepare_data,
    intersect_data,
    build_and_publish_tables
)

# File path constants
LOG_FILE = os.path.join('logs', 'app.log')
BASIC_SETTINGS_FILE = os.path.join('settings', 'basic_settings.yaml')
ADVANCED_SETTINGS_FILE = os.path.join('settings', 'advanced_settings.yaml')
SOURCE_DATA_PATH = os.path.join('data', 'source_data')
SOURCE_DATA_CONFIG = os.path.join('data_configs', 'source_data_config.yaml')
PREPARED_DATA_CONFIG = os.path.join('data_configs', 'prepared_data_config.yaml')
INTERSECTION_TABLES_CONFIG = os.path.join('data_configs', 'intersection_config.yaml')
PUBLISHING_CONFIG = os.path.join('data_configs', 'publishing_config.yaml')
OUTPUT_FOLDER = 'output'

logger = logging.getLogger(__name__)

# Flags to enable or disable various features
# For debugging only; all should be true for typical use or errors may occur.
LOGGING_ENABLED = True
LOAD_SETTINGS_ENABLED = True
STARTUP_TASKS_ENABLED = True
DATABASE_CONNECTION_ENABLED = True
COLLECT_DATA_ENABLED = True
PREPARE_DATA_ENABLED = True
INTERSECTION_TABLES_ENABLED = True
PUBLISHING_ENABLED = True

LOG_LEVEL = logging.DEBUG

def main() -> None:
    try:
        print("Program Start\n")
        if LOGGING_ENABLED:
            initialize_logger(LOG_LEVEL, LOG_FILE)
        if LOAD_SETTINGS_ENABLED:
            basic_settings, advanced_settings = load_settings(BASIC_SETTINGS_FILE, ADVANCED_SETTINGS_FILE)
        if STARTUP_TASKS_ENABLED:
            run_startup_tasks(advanced_settings)
        if DATABASE_CONNECTION_ENABLED:
            db_engine = connect_to_database(advanced_settings['database_url'])
        if COLLECT_DATA_ENABLED:
            data_source_manager = collect_primary_data(
                SOURCE_DATA_PATH,
                SOURCE_DATA_CONFIG,
                db_engine,
                sources_to_collect=basic_settings['sources_to_collect']
            )
        if PREPARE_DATA_ENABLED:
            data_processing_manager = prepare_data(
                PREPARED_DATA_CONFIG,
                db_engine,
                data_to_prepare=basic_settings['data_to_prepare']
            )
        if INTERSECTION_TABLES_ENABLED:
            intersection_tables_manager = intersect_data(
                intersection_tables_config_path=INTERSECTION_TABLES_CONFIG,
                intersection_col_names=advanced_settings['intersection_table_column_names'],
                db_engine=db_engine,
                intersection_tables_settings=basic_settings['tables_to_intersect']
            )
        if PUBLISHING_ENABLED:
            publishing_manager = build_and_publish_tables(
                publishing_config_path=PUBLISHING_CONFIG,
                db_engine=db_engine,
                tables_to_publish_settings=basic_settings['tables_to_publish']
            )
            logging.getLogger(__name__).info("Program completed successfully")
    except Exception as e:
        logging.getLogger(__name__).critical(f"Unexpected error in main; ending program\n {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
