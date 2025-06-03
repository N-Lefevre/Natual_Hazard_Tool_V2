"""
logger_config.py
Contains the configure_logging function to set up the application's logging configuration.
Provides a standard line length for dividing log messages.
"""

import logging

LOG_DIVISION = "-" * 80 # How wide to make the dividing line of dashes in the log

def configure_logging(log_level: int, log_file: str = 'logs/app.log') -> logging.Logger:
    """
    Configures the root logger for the application.

    Args:
        log_level (int): The numeric logging level (e.g., logging.DEBUG, logging.INFO).
        log_file (str): The file path where logs should be written.

    Returns:
        logging.Logger: The configured root logger.

    Raises:
        OSError: If the log file cannot be opened for writing.
    """
    logger = logging.getLogger()
    logger.setLevel(log_level)

    while logger.handlers:
        logger.handlers.pop()

    stream_handler = logging.StreamHandler()
    try:
        file_handler = logging.FileHandler(log_file)
    except Exception as e:
        print(f"Failed to create log file handler: {e}")
        raise

    stream_handler.setLevel(log_level)
    file_handler.setLevel(log_level)

    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%y-%m-%d %H:%M:%S'
    )
    simple_formatter = logging.Formatter('%(message)s')

    if log_level == logging.INFO:
        stream_handler.setFormatter(simple_formatter)
    else:
        stream_handler.setFormatter(detailed_formatter)

    file_handler.setFormatter(detailed_formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger