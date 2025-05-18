# pylint: disable=line-too-long
# pylint: disable=too-many-lines
# pylint: disable=missing-module-docstring
# pylint: disable=invalid-name
# pylint: disable=consider-using-enumerate
# pylint: disable=line-too-long
from datetime import datetime
import re


class vTools:
    """Library of functions used in classes of project"""

    @staticmethod
    def check_INT(varin):
        """Check if supplied value is int

        Args:
            varin (int): value to test

        Returns:
            bool: true if input value is int else false
        """
        try:
            return isinstance(int(varin), int)
        except (ValueError, TypeError):
            return False

    @staticmethod
    def check_STR(varin):
        """Check if supplied value is str

        Args:
            varin (str): value to test

        Returns:
            bool: true if input value is str else false
        """
        try:
            return isinstance(varin, str)
        except (ValueError, TypeError):
            return False

    @staticmethod
    def check_FLOAT(varin):
        """Check if supplied value is float

        Args:
            varin (float): value to test

        Returns:
            bool: true if input value is float else false
        """
        try:
            return isinstance(float(varin), float)
        #     reg = re.search(r"^[-+]?(\d+([.,]\d*)?|[.,]\d+)([eE][-+]?\d+)?$", varin)
        #     return bool(reg is not None)
        except (ValueError, TypeError):
            return False

    @staticmethod
    def check_HEX(varin):
        """Check if supplied value is hex

        Args:
            varin (hex): value to test

        Returns:
            bool: true if input value is hex else false
        """
        try:
            reg = re.search(r"^[-+]?(0[xX][\dA-Fa-f]+|0[0-7]*|\d+)$", varin)
            return bool(reg is not None)
        except (ValueError, TypeError):
            return False

    @staticmethod
    def check_DATETIME(varin):
        """Check if supplied value is datetime

        Args:
            varin (datetime): value to test

        Returns:
            bool: true if input value is datetime else false
        """
        try:
            reg = datetime.fromtimestamp(varin, tz=None)
            return bool(reg is not None)
        except (ValueError, TypeError):
            return False
