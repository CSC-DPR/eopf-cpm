from functools import wraps
from re import compile
from typing import Any, Callable, Dict, Tuple, Union

from eopf.exceptions import FormattingDecoratorMissingUri
from eopf.exceptions.warnings import FormatterAlreadyRegistered
from eopf.formatting.formatters import EOAbstractFormatter


class EOFormatterFactory(object):
    """
    Factory for formatters

    Parameters
    ----------
    default_formatters: bool
        If True the default way of registering formatters are used

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
        self._formatters: Dict[str, type[EOAbstractFormatter]] = dict()
        if default_formatters:
            from eopf.formatting.formatters import (
                Text,
                ToBbox,
                ToBool,
                ToFloat,
                ToGeoJson,
                ToImageSize,
                ToInt,
                ToISO8601,
                ToStr,
                ToUNIXTimeSLSTRL1,
            )

            self.register_formatter(ToStr)
            self.register_formatter(ToFloat)
            self.register_formatter(ToBool)
            self.register_formatter(ToUNIXTimeSLSTRL1)
            self.register_formatter(ToISO8601)
            self.register_formatter(ToBbox)
            self.register_formatter(ToGeoJson)
            self.register_formatter(ToInt)
            self.register_formatter(Text)
            self.register_formatter(ToImageSize)
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
        formatter_name = str(formatter.name)
        if formatter_name in self._formatters.keys():
            raise FormatterAlreadyRegistered(f"{formatter_name} already registered")
        self._formatters[formatter_name] = formatter

    def get_formatter(
        self,
        path: Any,
    ) -> Tuple[Union[str, None], Union[Callable[[EOAbstractFormatter], Any], None], Any]:
        """
        Function retrieve a formatter and path without the formatter pattern

        Parameters
        ----------
        path: Any
            a path to an object/file


        Returns
        ----------
        Tuple[Union[str, None], Union[Callable[[EOAbstractFormatter], Any], None], Any]:
            A tuple containing the formatter name, the formatting method and
            a the path (without the formatter name)
        """

        # try to get a string representation of the path
        try:
            str_repr = str(path)
        except:  # noqa
            # path can not be searched and is passed to the reader/accessor as is
            return None, None, path

        # build regex expression for formatters
        registered_formaters = "|".join(self._formatters.keys())
        regex = compile("^(.+:/{2,})?(%s)\\((.+)\\)" % registered_formaters)
        # check if regex matches
        m = regex.match(str_repr)
        if m:
            prefix = m[1]
            formatter_name = m[2]
            inner_path = m[3]

            if prefix:
                return formatter_name, self._formatters[formatter_name]().format, prefix + inner_path
            else:
                return formatter_name, self._formatters[formatter_name]().format, inner_path

        else:
            # no formatter pattern found
            return None, None, path


def formatable_func(fn: Callable[[Any], Any]) -> Any:
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

    @wraps(fn)
    def wrap(*args: Any, **kwargs: Any) -> Any:
        """Any function that received a path, url or key can be formatted"""

        if not len(args) >= 1:
            raise FormattingDecoratorMissingUri("The decorated function does not contain a URI")

        # parse the path, which should always be the first argument
        _, formatter, formatter_stripped_uri = EOFormatterFactory().get_formatter(args[0])

        # replace the first argument with the formatter_stripped_uri
        lst_args = list(args)
        lst_args[0] = formatter_stripped_uri
        new_args = tuple(lst_args)

        # call the decorated function
        decorated_func_ret = fn(*new_args, **kwargs)
        if formatter is not None:
            return formatter(decorated_func_ret)
        return decorated_func_ret

    return wrap


class formatable_method(object):
    """Decorator class to allow class methods to allow formating of the return\

    Parameters
    ----------
    fn: Callable[[Any], Any]
        a method of class which has a return

    Attributes
    ----------
    fn: Callable[[Any], Any]
        a method of class which has a return
    parent_obj: Any
        the object coresponding to the decorated method

    Examples
    ----------
    class example(object):
        def __init__(self, val:int):
            self.d: Dict[str, int] = {"a_val": val}

        @formatable_method
        def get_val(self, url: str):
            return self.d[url]

    ex = example(2)
    ex.get_val("to_str(a_val)")
    """

    def __init__(self, fn: Callable[[Any], Any], decorator_factory: EOFormatterFactory = None) -> None:
        self.fn = fn
        self.parent_obj: object = None

        if decorator_factory:
            self.decorator_factory = decorator_factory
        else:
            self.decorator_factory = EOFormatterFactory()

    def __get__(self, obj: object, _: Any = None) -> "formatable_method":
        self.parent_obj = obj
        return self

    def __call__(self, *args: Any, **kwargs: Any) -> Any:

        if not len(args) >= 1:
            raise FormattingDecoratorMissingUri("The decorated function does not contain a URI")

        # parse the path, which should always be the first argument
        _, formatter, formatter_stripped_uri = self.decorator_factory.get_formatter(args[0])
        # replace the first argument with the formatter_stripped_uri
        lst_args = list(args)
        lst_args[0] = formatter_stripped_uri
        new_args = tuple(lst_args)

        # call the decorated function
        decorated_method_ret = self.fn(self.parent_obj, *new_args, **kwargs)
        if formatter is not None:
            return formatter(decorated_method_ret)

        return decorated_method_ret
