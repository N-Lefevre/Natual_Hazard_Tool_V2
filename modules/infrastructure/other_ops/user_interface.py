"""
user_interface.py
Contains the ConsoleDisplay class to handle console display operations.
"""

import logging
from modules.infrastructure.other_ops.file_operations import read_text_file

logger = logging.getLogger(__name__)

class ConsoleDisplay:
    """
    Class to handle console display operations.
    """

    @staticmethod
    def print_text_file(file_path: str, desc: str = '') -> None:
        """
        Print the content of a text file to the console.

        Args:
            file_path (str): The path to the text file to be printed.
            desc (str): A description of the file being printed, used for logging purposes.

        Raises:
            FileNotFoundError: If the file does not exist.
            Exception: If the file cannot be read or printed.
        """
        try:
            content = read_text_file(file_path)

            if not content.strip():
                msg = f"The file is empty: {file_path}"
                if desc:
                    msg += f" ({desc})"
                logger.warning(msg)
                print(f"[{desc}] The file is empty.\n" if desc else "The file is empty.\n")
                return

            print(content, end='\n')
            logger.debug(f"Successfully displayed {desc} from: {file_path}" if desc else f"Successfully displayed file: {file_path}")
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}" + (f" ({desc})" if desc else ""))
            raise
        except Exception as e:
            logger.error(f"Cannot display {desc} from {file_path}: {e}" if desc else f"Cannot display file {file_path}: {e}")
            raise