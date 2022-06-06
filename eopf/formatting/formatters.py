from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from pandas import Timedelta, to_datetime
from pytz import UTC

from eopf.product.core import EOVariable


class EOAbstractFormatter(ABC):
    """Abstract formatter representation
    Attributes
    ----------
    name: str
        name of the formatter
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Set the name of the formatter, for registering it"""
        raise NotImplementedError()

    @abstractmethod
    def format(self, input: Any, **kwargs: Any) -> Any:
        """Function that returns the formmated input"""
        raise NotImplementedError()


class to_str(EOAbstractFormatter):

    name = "to_str"

    def format(self, input: Any, **kwargs: Any) -> str:
        try:
            return str(input)
        except:
            raise KeyError("to be modified")


class to_float(EOAbstractFormatter):

    name = "to_float"

    def format(self, input: Any, **kwargs: Any) -> float:
        try:
            return float(input)
        except:
            raise KeyError("to be modified")


class to_int(EOAbstractFormatter):

    name = "to_int"

    def format(self, input: Any, **kwargs: Any) -> float:
        try:
            return int(input)
        except:
            raise KeyError("to be modified")


class to_unix_time_slstr_l1(EOAbstractFormatter):

    name = "to_unix_time_slstr_l1"

    def format(self, input: Any, **kwargs: Any) -> EOVariable:

        start = to_datetime(datetime.fromtimestamp(0, tz=UTC))
        end = to_datetime(input)
        # compute and convert the time difference into microseconds
        time_delta = (end - start) // Timedelta("1microsecond")

        # create coresponding attributes
        attributes = {}
        attributes["unit"] = f"microseconds since {start.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}"
        attributes["standard_name"] = "time"
        if "key" in kwargs:
            key = kwargs['key']
            if key == "ANX_time":
                attributes["long_name"] = "Time of ascending node crossing in UTC"
            elif key == "calibration_time":
                attributes["long_name"] = "Time of calibration in UTC"

        # create an EOVariable and return it
        eov: EOVariable = EOVariable(data=time_delta, attrs=attributes)
        return eov


class to_iso8601(EOAbstractFormatter):

    name = "to_iso8601"

    def format(self, input: str, **kwargs: Any) -> str:
        """Convert time to ISO8601 standard, E.g: 20220506T072719 -> 2022-05-06T07:27:19Z

        Parameters
        ----------
        input: str
            xpath

        Returns
        ----------
        date_string: strftime (string-like time format)
            String containing date converted to ISO standard.
        """

        dt_obj = datetime.strptime(input, "%Y%m%dT%H%M%S")
        date_string = dt_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
        return date_string
