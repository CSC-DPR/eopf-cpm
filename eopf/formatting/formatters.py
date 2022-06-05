from typing import Any
from abc import ABC, abstractmethod


class EOAbstractFormatter(ABC):

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def format(self, input: Any, **kwargs: Any) -> Any:
        raise NotImplementedError()


class to_str(EOAbstractFormatter):

    name = "to_str"

    def format(self, input: Any, **kwargs: Any) -> str:
        try:
            return str(input)
        except:
            raise KeyError("to be modified")  


class to_str_times(EOAbstractFormatter):

    name = "to_str_times"

    def format(self, input: Any, **kwargs: Any) -> str:

        if 'times' in kwargs:
            times = kwargs['times']
        try:
            return str(input) * times
        except:
            raise KeyError("to be modified")  


# def to_str(non_string: Any) -> str:
#     try:
#         return str(non_string)
#     except:
#         raise KeyError("to be modified")