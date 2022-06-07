from re import compile
from typing import Any, Callable, Dict, Optional, Tuple, Union

from eopf.formatting.formatters import EOAbstractFormatter


class EOFormatterFactory(object):
    """
    Factory for formatters

    Parameters
    ----------
    default_formatters: bool
        ???

    Attributes
    ----------
    formatters: Dict[str, type[EOAbstractFormatter]]
        dictonary of formatters

    Examples
    ----------
    def get_the_data(path):
        ...

    formatter, path = EOAbstractFormatter().get_formatter("to_str(/tmp/some_file.nc)")
    the_data = get_the_data(path)
    if formatter:
        return formatter(the_data)
    return the_data
    """

    def __init__(self, default_formatters: bool = True) -> None:
        self.formatters: Dict[str, type[EOAbstractFormatter]] = dict()
        if default_formatters:
            from eopf.formatting.formatters import (
                to_float,
                to_int,
                to_iso8601,
                to_str,
                to_unix_time_slstr_l1,
            )

            self.register_formatter(to_str)
            self.register_formatter(to_float)
            self.register_formatter(to_int)
            self.register_formatter(to_unix_time_slstr_l1)
            self.register_formatter(to_iso8601)
        else:
            # to implement another logic of importing formatters
            pass

    def register_formatter(self, formatter: type[EOAbstractFormatter]) -> None:
        """
        Function to register new formatters

        Parameters
        ----------
        formatter: type[EOAbstractFormatter]
            a formatter
        """
        key = str(formatter.name)
        self.formatters[key] = formatter

    def get_formatter(self, path: Any) -> Tuple[Union[Callable[[EOAbstractFormatter], Any], None], Any]:
        """
        Function retrieve a formatter and path without the formatter pattern

        Parameters
        ----------
        path: Any
            a path to an object/file
        """

        # try to get a string representation of the path
        try:
            str_repr = str(path)
        except:  # noqa
            # path can not be searched and is passed to the reader/accessor as is
            return None, path

        # build regex expression for formatters
        registered_formaters = "|".join(self.formatters.keys())
        regex = compile("^(.+://)?(%s)\\((.+)\\)" % registered_formaters)
        # check if regex matches
        m = regex.match(str_repr)
        if m:
            prefix = m[1]
            formatter_name = m[2]
            inner_path = m[3]

            if prefix:
                return self.formatters[formatter_name]().format, prefix + inner_path
            else:
                return self.formatters[formatter_name]().format, inner_path

        else:
            # no formatter pattern found
            return None, path


def formatable(fn: Callable[[Any], Any]) -> Any:
    """Decorator function used to allow formating of the return

    Parameters
    ----------
    fn: Callable[[Any], Any]
        callable function

    Returns
    ----------
    Any: formated return of the wrapped function

    Examples
    --------
    >>> @formatable
    >>> def get_data(key, a_float):
    ...     unformatted_data = read_data(key)
    ...     return unformatted_data * a_float
    ...
    >>> ret = get_data('to_float(/tmp/data.nc)', a_float=3.14)
    """

    def wrap(path: Optional[Any] = None, url: Optional[Any] = None, key: Optional[Any] = None, **kwargs: Any) -> Any:
        """Any function that received a path, url or key can be formatted"""

        if path:
            uri = path
        elif url:
            uri = url
        elif key:
            uri = key
        else:
            raise KeyError("No key provided")

        formatter, formatter_stripped_uri = EOFormatterFactory().get_formatter(uri)
        print(formatter, formatter_stripped_uri)
        the_data = fn(formatter_stripped_uri, **kwargs)
        if formatter:
            return formatter(the_data, **kwargs)
        return the_data

    return wrap
