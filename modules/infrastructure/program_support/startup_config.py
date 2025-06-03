"""
startup_config.py
Contains the Startup class to handle the startup sequence of the application.
"""

import logging
from modules.infrastructure.other_ops.user_interface import ConsoleDisplay

logger = logging.getLogger(__name__)

class Startup:
    """
    Class to handle the startup sequence of the application.
    Displays program information, ASCII art, and instructions.

    Attributes:
        program_info_file (str): Path to the program information file.
        ascii_art_file (str): Path to the ASCII art file.
        instructions_file (str): Path to the instructions file.
    """
    def __init__(self, program_info_file: str, ascii_art_file: str, instructions_file: str):
        """
        Initialize the Startup class with file paths.

        Args:
            program_info_file (str): Path to the program information file.
            ascii_art_file (str): Path to the ASCII art file.
            instructions_file (str): Path to the instructions file.
        """
        self.program_info_file = program_info_file
        self.ascii_art_file = ascii_art_file
        self.instructions_file = instructions_file

        logger.debug('Startup initialized.')

    def display_startup_info(self) -> None:
        """
        Display the startup information by printing the content of the provided files.
        Logs errors for each file individually but continues displaying others.
        """
        for file_path, desc in [
            (self.ascii_art_file, "EPA ASCII art"),
            (self.program_info_file, "Program info"),
            (self.instructions_file, "Instructions file"),
        ]:
            try:
                ConsoleDisplay.print_text_file(file_path, desc=desc)
            except Exception as e:
                logger.error(f"Error displaying {desc} from {file_path}: {e}")
        logger.debug('Startup information displayed successfully.')