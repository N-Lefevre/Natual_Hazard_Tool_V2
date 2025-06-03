"""
settings_config.py

Contains the SettingsManager class, which is a singleton responsible for managing application settings.
"""

import logging
from typing import Optional, Dict, Any
from modules.infrastructure.other_ops.file_operations import read_yaml_file

logger = logging.getLogger(__name__)

class SettingsManager:
    """
    Singleton class to manage application settings.
    Loads settings from YAML files and provides access to them.

    Attributes:
        basic_settings_file (str): Path to the basic settings YAML file.
        advanced_settings_file (str): Path to the advanced settings YAML file.
        _basic_settings (dict): Loaded basic settings.
        _advanced_settings (dict): Loaded advanced settings.
    """
    _instance: Optional["SettingsManager"] = None
    basic_settings_file: str
    advanced_settings_file: str
    _basic_settings: Dict[str, Any]
    _advanced_settings: Dict[str, Any]

    def __new__(cls, basic_settings_file: str = 'settings/basic_settings.yaml', advanced_settings_file: str = 'settings/advanced_settings.yaml') -> "SettingsManager":
        """
        Create a new instance of SettingsManager if it doesn't exist.
        Load settings from files during initialization.

        Args:
            basic_settings_file (str): Path to the basic settings YAML file.
            advanced_settings_file (str): Path to the advanced settings YAML file.

        Returns:
            SettingsManager: Singleton instance of the class.

        Raises:
            Exception: If loading settings fails.
        """
        if cls._instance is None:
            cls._instance = super(SettingsManager, cls).__new__(cls)
            cls._instance.basic_settings_file = basic_settings_file
            cls._instance.advanced_settings_file = advanced_settings_file
            try:
                cls._instance._load_settings()
            except Exception:
                logger.critical("CRITICAL: Failed to load one or more settings files")
                raise
        return cls._instance

    def _load_settings(self) -> None:
        """
        Load settings from YAML files.
        Raises an exception if any of the files cannot be loaded.

        Raises:
            Exception: If loading either settings file fails.
        """
        try:
            self._basic_settings = read_yaml_file(self.basic_settings_file)
            logger.debug("Basic settings loaded successfully")
        except Exception:
            logger.critical(f"CRITICAL: Failed to load basic settings from {self.basic_settings_file}")
            raise

        try:
            self._advanced_settings = read_yaml_file(self.advanced_settings_file)
            logger.debug("Advanced settings loaded successfully")
        except Exception as e:
            logger.critical(f"Failed to load advanced settings from {self.advanced_settings_file}: {e}")
            raise

    @property
    def basic_settings(self) -> Dict[str, Any]:
        """
        Get the basic settings.

        Returns:
            dict: Basic settings loaded from the YAML file.
        """
        return self._basic_settings

    @property
    def advanced_settings(self) -> Dict[str, Any]:
        """
        Get the advanced settings.

        Returns:
            dict: Advanced settings loaded from the YAML file.
        """
        return self._advanced_settings