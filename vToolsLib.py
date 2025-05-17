# pylint: disable=line-too-long
# pylint: disable=too-many-lines
# pylint: disable=missing-module-docstring
# pylint: disable=invalid-name
# pylint: disable=consider-using-enumerate
# pylint: disable=line-too-long
from datetime import datetime
import re


class vTools:
    @staticmethod
    def check_INT(varin):
        try:
            return isinstance(int(varin), int)
        except (ValueError, TypeError):
            return False

    @staticmethod
    def check_STR(varin):
        try:
            return isinstance(varin, str)
        except (ValueError, TypeError):
            return False

    @staticmethod
    def check_FLOAT(varin):
        try:
            return isinstance(float(varin), float)
        #     reg = re.search(r"^[-+]?(\d+([.,]\d*)?|[.,]\d+)([eE][-+]?\d+)?$", varin)
        #     return bool(reg is not None)
        except (ValueError, TypeError):
            return False

    @staticmethod
    def check_HEX(varin):
        try:
            reg = re.search(r"^[-+]?(0[xX][\dA-Fa-f]+|0[0-7]*|\d+)$", varin)
            return bool(reg is not None)
        except (ValueError, TypeError):
            return False

    @staticmethod
    def check_DATETIME(varin):
        try:
            reg = datetime.fromtimestamp(varin, tz=None)
            return bool(reg is not None)
        except (ValueError, TypeError):
            return False
