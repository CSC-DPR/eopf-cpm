from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, List

from lxml.etree import _ElementUnicodeResult
from pandas import Timedelta, to_datetime
from pytz import UTC

from eopf.exceptions import FormattingError
from eopf.product.core import EOVariable

from .utils import detect_pole_or_antemeridian, poly_coords_parsing, split_poly


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
        except Exception as e:
            raise FormattingError(f"{e}")


class Text(EOAbstractFormatter):

    name = "Text"

    def format(self, input: Any, **kwargs: Any) -> str:
        return input


class to_float(EOAbstractFormatter):

    name = "to_float"

    def format(self, input: Any, **kwargs: Any) -> float:
        try:
            return float(input)
        except Exception as e:
            raise FormattingError(f"{e}")


class to_int(EOAbstractFormatter):

    name = "to_int"

    def format(self, input: Any, **kwargs: Any) -> float:
        try:
            return int(input)
        except Exception as e:
            raise FormattingError(f"{e}")


class to_bool(EOAbstractFormatter):

    name = "to_bool"

    def format(self, input: Any, **kwargs: Any) -> bool:
        try:
            return bool(input)
        except Exception as e:
            raise FormattingError(f"{e}")


class to_unix_time_slstr_l1(EOAbstractFormatter):

    name = "to_unix_time_slstr_l1"

    def format(self, input: Any, **kwargs: Any) -> EOVariable:

        start = to_datetime(datetime.fromtimestamp(0, tz=UTC))
        end = to_datetime(input[:])
        # compute and convert the time difference into microseconds
        time_delta = (end - start) // Timedelta("1microsecond")

        # create coresponding attributes
        attributes = {}
        attributes["unit"] = f"microseconds since {start.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}"
        attributes["standard_name"] = "time"

        if input.name == "ANX_time":
            attributes["long_name"] = "Time of ascending node crossing in UTC"
        elif input.name == "calibration_time":
            attributes["long_name"] = "Time of calibration in UTC"

        # create an EOVariable and return it
        eov: EOVariable = EOVariable(data=time_delta, attrs=attributes)
        return eov


class to_iso8601(EOAbstractFormatter):

    name = "to_ISO8601"

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


class to_bbox(EOAbstractFormatter):

    name = "to_bbox"

    def format(self, input: Any, **kwargs: Any) -> List[float]:
        """Computes coordinates of a polygon bounding box

        Parameters
        ----------
        path: str
            xpath

        Returns
        ----------
        List[float]:
            Returns a list with coordinates, longitude, latitude for SW/NE points
        """

        poly_coords = poly_coords_parsing(a_string=input)

        # Maybe use to_geojson to get coordiates
        max_lon = max(poly_coords, key=lambda x: x[0])[0]  # Return tuple with biggest value on index 0
        min_lon = min(poly_coords, key=lambda x: x[0])[0]  # Return tuple with smallest value on index 0
        max_lat = max(poly_coords, key=lambda x: x[1])[1]  # Return tuple with biggest value on index 1
        min_lat = min(poly_coords, key=lambda x: x[1])[1]  # Return tuple with smallest value on index 1
        return [max_lon, min_lat, min_lon, max_lat]  # Order to be reviewed


class to_geojson(EOAbstractFormatter):

    name = "to_geoJson"

    def format(self, input: Any, **kwargs: Any) -> Any:

        #poly_coords_str = input.text
        poly_coords_str = input.text
        poly_coords = poly_coords_parsing(poly_coords_str)
        # If polygon coordinates crosses any pole or antemeridian, split the polygon in a mulitpolygon
        if detect_pole_or_antemeridian(poly_coords):
            return dict(type="MultiPolygon", coordinates=split_poly(poly_coords))
        # Otherwise, just return computed coordinates
        return dict(type="Polygon", coordinates=[poly_coords])
