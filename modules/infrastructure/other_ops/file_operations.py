"""
file_operations.py
Contains functions for file operations such as reading YAML files, reading text files, and saving data to GeoPackage files.
"""

import yaml
import logging
import os
import geopandas as gpd

logger = logging.getLogger(__name__)

def read_yaml_file(file_path: str) -> dict:
    """
    Reads a YAML file and returns its content.

    Args:
        file_path (str): The path to the YAML file.

    Returns:
        dict: The content of the YAML file.

    Raises:
        FileNotFoundError: If the file does not exist.
        yaml.YAMLError: If the file is not valid YAML.
        Exception: For other I/O errors.
    """
    logger.debug(f"Attempting to open yaml file at {file_path}")
    try:
        with open(file_path, 'r') as file:
            content = yaml.safe_load(file) or {}
        if not isinstance(content, dict):
            logger.error(f"YAML file at {file_path} does not contain a dictionary at the root.")
            raise ValueError(f"YAML file at {file_path} does not contain a dictionary at the root.")
    except FileNotFoundError:
        logger.error(f"YAML file not found: {file_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"YAML parsing error in {file_path}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error reading YAML file {file_path}: {e}")
        raise
    else:
        logger.debug(f"YAML file at {file_path} opened and parsed successfully")
        return content

def read_text_file(file_path: str) -> str:
    """
    Reads and returns the content of a text file.

    Args:
        file_path (str): The path to the text file.

    Returns:
        str: The content of the text file.

    Raises:
        FileNotFoundError: If the file does not exist.
        Exception: For other I/O errors.
    """
    logger.debug(f"Attempting to open txt file at {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
    except FileNotFoundError:
        logger.error(f'{file_path} file not found.')
        raise
    except Exception as e:
        logger.error(f'An error occurred while reading {file_path}: {e}')
        raise
    else:
        logger.debug(f"txt file at {file_path} opened and parsed successfully")
        return content

def save_to_geopackage(
    data: gpd.GeoDataFrame,
    local_data_path: str,
    save_name: str,
    layer: str = None
    ) -> str:
    """
    Saves data to a GeoPackage file.

    Args:
        data (GeoDataFrame): The data to be saved.
        local_data_path (str): The directory where the GeoPackage will be saved.
        save_name (str): The name of the GeoPackage file.
        layer (str, optional): The layer name for the GeoPackage. Defaults to None.

    Returns:
        str: The path to the saved GeoPackage file.

    Raises:
        ValueError: If data is not a GeoDataFrame.
        Exception: If saving fails.
    """
    logger.debug(f"Attempting to save data to GeoPackage at {local_data_path}")
    if not isinstance(data, gpd.GeoDataFrame):
        logger.error("Provided data is not a GeoDataFrame.")
        raise ValueError("Provided data is not a GeoDataFrame.")
    try:
        os.makedirs(local_data_path, exist_ok=True)
        output_path = os.path.join(local_data_path, save_name)
        data.to_file(output_path, driver='GPKG', layer=layer)
        logger.debug(f"Data saved to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Failed to save data to GeoPackage at {local_data_path}: {e}")
        raise