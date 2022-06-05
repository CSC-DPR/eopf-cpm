from typing import Callable, Any, Dict, Tuple, Union
from re import match

from eopf.formatting.formatters import EOAbstractFormatter

# class EOFormatterFactory:
#     def __init__(self, default_formatters = True) -> None:
#         self.formatters = dict()
#         if default_formatters:
#             from eopf.formatting.formatters import to_str
#             self.register_formatter(to_str)

#     def register_formatter(self, func: Callable) -> None:
#         self.formatters[func.__name__] = func

#     def get_formatter(self, path: Any) -> None:
#         try:
#             str_repr = str(path)
#         except:
#             return None, path

#         m = match(r"^(to_\w+)\((.+)\)", str_repr)
#         if m:
#             print(self.formatters, m[1])
#             if m[1] in self.formatters:
#                 return self.formatters[m[1]], m[2]
#             else:
#                 raise KeyError(f"no registered formatter {m[1]}")
#         else:
#             return None, path


class EOFormatterFactory(object):
    def __init__(self, default_formatters: bool = True) -> None:
        self.formatters: Dict[str, type[EOAbstractFormatter]] = dict()
        if default_formatters:
            from eopf.formatting.formatters import to_str
            self.register_formatter(to_str)
            from eopf.formatting.formatters import to_str_times
            self.register_formatter(to_str_times)

    def register_formatter(self, formatter: type[EOAbstractFormatter]) -> None:
        key = str(formatter.name)
        self.formatters[key] = formatter

    def get_formatter(self, path: Any) -> Tuple[Union[Callable[[EOAbstractFormatter], Any], None], Any]:
        try:
            str_repr = str(path)
        except:
            return None, path

        m = match(r"^(s3://)?(to_\w+)\((.+)\)", str_repr)
        if m:
            s3_prefix = m[1]
            formatter_name = m[2]
            inner_path = m[3]

            if formatter_name in self.formatters:
                if s3_prefix:
                    return self.formatters[formatter_name]().format, s3_prefix + inner_path
                else:
                    return self.formatters[formatter_name]().format, inner_path
            else:
                raise KeyError(f"no registered formatter {m[1]}")
        else:
            return None, path




