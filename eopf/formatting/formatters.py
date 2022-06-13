from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Union

from pandas import Timedelta, to_datetime
from numpy import int64
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
    def format(self, input: Any) -> Any:
        """Function that returns the formmated input"""
        raise NotImplementedError()


class to_str(EOAbstractFormatter):
    """Formatter for string conversion"""

    # docstr-coverage: inherited
    name = "to_str"

    # docstr-coverage: inherited
    def format(self, input: Any) -> str:
        """Convert input to string

        Parameters
        ----------
        input: Any

        Returns
        ----------
        str:
            String representation of the input

        Raises
        ----------
        FormattingError
            When formatting can not be carried
        """
        try:
            return str(input)
        except Exception as e:
            raise FormattingError(f"{e}")


class to_float(EOAbstractFormatter):
    """Formatter for float conversion"""

    # docstr-coverage: inherited
    name = "to_float"

    def format(self, input: Any) -> float:
        """Convert input to float

        Parameters
        ----------
        input: Any

        Returns
        ----------
        float:
            Float representation of the input

        Raises
        ----------
        FormattingError
            When formatting can not be carried
        """
        try:
            return float(input)
        except Exception as e:
            raise FormattingError(f"{e}")


class to_int(EOAbstractFormatter):
    """Formatter for int conversion"""

    # docstr-coverage: inherited
    name = "to_int"

    def format(self, input: Any) -> int:
        """Convert input to int

        Parameters
        ----------
        input: Any

        Returns
        ----------
        int:
            Integer representation of the input

        Raises
        ----------
        FormattingError
            When formatting can not be carried
        """
        try:
            return int(input)
        except Exception as e:
            raise FormattingError(f"{e}")


class to_bool(EOAbstractFormatter):
    """Formatter for bool conversion"""

    # docstr-coverage: inherited
    name = "to_bool"

    def format(self, input: Any) -> bool:
        """Convert input to boolean

        Parameters
        ----------
        input: Any

        Returns
        ----------
        bool:
            Boolean representation of the input

        Raises
        ----------
        FormattingError
            When formatting can not be carried
        """
        try:
            return bool(input)
        except Exception as e:
            raise FormattingError(f"{e}")


class to_unix_time_slstr_l1(EOAbstractFormatter):
    """Formatter for unix time conversion for SLSTR L1 ANX_time and calibration_time variables"""

    # docstr-coverage: inherited
    name = "to_unix_time_slstr_l1"

    def format(self, input: Any) -> EOVariable:
        """Convert input to unix time

        Parameters
        ----------
        input: Any

        Returns
        ----------
        eov: EOVariable
            EOVariable with the data converted to unix time

        Raises
        ----------
        FormattingError
            When formatting can not be carried
        """
        try:
            # compute the start and end time
            start = to_datetime(datetime.fromtimestamp(0, tz=UTC))
            end = to_datetime(input[:])

            # compute and convert the time difference into microseconds
            time_delta = int64((end - start) // Timedelta("1microsecond"))
            print(type(time_delta))

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
        except Exception as e:
            raise FormattingError(f"{e}")


class to_iso8601(EOAbstractFormatter):
    """Formatter for ISO8601 (time) conversion"""

    # docstr-coverage: inherited
    name = "to_ISO8601"

    def format(self, input: str) -> str:
        """Convert time to ISO8601 standard, E.g: 20220506T072719 -> 2022-05-06T07:27:19Z

        Parameters
        ----------
        input: str
            xpath

        Returns
        ----------
        date_string: strftime (string-like time format)
            String containing date converted to ISO standard.

        Raises
        ----------
        FormattingError
            When formatting can not be carried
        """
        try:
            dt_obj = datetime.strptime(input, "%Y%m%dT%H%M%S")
            date_string = dt_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
            return date_string
        except Exception as e:
            raise FormattingError(f"{e}")


class to_bbox(EOAbstractFormatter):
    """Formatter for computing coordinates of a polygon bounding box"""

    # docstr-coverage: inherited
    name = "to_bbox"

    # docstr-coverage: inherited
    def format(self, input: Any) -> List[float]:
        """Computes coordinates of a polygon bounding box

        Parameters
        ----------
        path: str
            xpath

        Returns
        ----------
        List[float]:
            Returns a list with coordinates, longitude, latitude for SW/NE points

        Raises
        ----------
        FormattingError
            When formatting can not be carried
        """
        try:
            poly_coords = poly_coords_parsing(a_string=input)

            # Maybe use to_geojson to get coordiates
            max_lon = max(poly_coords, key=lambda x: x[0])[0]  # Return tuple with biggest value on index 0
            min_lon = min(poly_coords, key=lambda x: x[0])[0]  # Return tuple with smallest value on index 0
            max_lat = max(poly_coords, key=lambda x: x[1])[1]  # Return tuple with biggest value on index 1
            min_lat = min(poly_coords, key=lambda x: x[1])[1]  # Return tuple with smallest value on index 1
            return [max_lon, min_lat, min_lon, max_lat]  # Order to be reviewed
        except Exception as e:
            raise FormattingError(f"{e}")


class to_geojson(EOAbstractFormatter):
    """Formatter for converting polygon coordinates to geoJson format"""

    # docstr-coverage: inherited
    name = "to_geoJson"

    # docstr-coverage: inherited
    def format(self, input: Any) -> Dict[str, Union[List[Any], str]]:
        """Computes a polygon / multipolygon coordinates in geoJson format,
        from xml acquired coordiantes

        Parameters
        ----------
        input: str

        Returns
        ----------
        List[List[float]]:
            Returns a list of lists(tuples) containg a pair (latitude, longitude) for each point of a polygon.

        Raises
        ----------
        FormattingError
            When formatting can not be carried
        """
        try:
            poly_coords_str = input
            poly_coords = poly_coords_parsing(poly_coords_str)
            # If polygon coordinates crosses any pole or antemeridian, split the polygon in a mulitpolygon
            if detect_pole_or_antemeridian(poly_coords):
                return dict(type="MultiPolygon", coordinates=split_poly(poly_coords))
            # Otherwise, just return computed coordinates
            return dict(type="Polygon", coordinates=[poly_coords])
        except Exception as e:
            raise FormattingError(f"{e}")


class to_imageSize(EOAbstractFormatter):
    """Silent Formatter used to read medata files"""

    # docstr-coverage: inherited
    name = "to_imageSize"

    def format(self, input: Any) -> Any:
        """Silent formmater, used only for parsing the path
        logic is present in stac_mapper method of XMLManifestAccessor

        Parameters
        ----------
        input: Any
            input

        Returns
        ----------
        Any:
            Returns the input
        """
        return input


class Text(EOAbstractFormatter):
    """Silent Formatter used to read medata files"""

    # docstr-coverage: inherited
    name = "Text"

    def format(self, input: Any) -> Any:
        """Silent formmater, used only for parsing the path
        logic is present in stac_mapper method of XMLManifestAccessor

        Parameters
        ----------
        input: Any
            input

        Returns
        ----------
        Any:
            Returns the input
        """
        return input
