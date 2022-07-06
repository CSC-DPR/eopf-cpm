import json
from abc import ABC, abstractmethod
from typing import Any, Iterator, Sequence

from eopf.exceptions import EOTriggeringConfigurationError


class ParseLoader(ABC):
    """Abstract calss to Parse triggering payload data"""

    def load(self, data_to_parse: Any) -> Any:
        """Load the data to provide a full plain dict object (follow link).

        Parameters
        ----------
        data_to_parse: str or dict or list
            if is a string, the data is load with json.load

        Returns
        -------
        json loaded object
        """
        if isinstance(data_to_parse, str):
            with open(data_to_parse) as f:
                data_to_parse = json.load(f)
        return data_to_parse

    @abstractmethod
    def parse(self, data_to_parse: Any, **kwargs: Any) -> Any:
        """Retrieve and parse the given data for the dedicated Key

        Parameters
        ----------
        data_to_parse

        Returns
        -------
        parsed items
        """


class EOTriggeringKeyParser(ParseLoader):
    """Abstract class to parse key in triggering payload data"""

    KEY: str = ""
    """Corresponding key in the payload"""
    OPTIONAL: bool = False
    """this key can not be here ?"""
    DEFAULT: Any = {}
    """default for optional if not present"""
    OPTIONAL_KEYS: Sequence[str] = tuple()
    """subkeys not mandatory"""
    MANDATORY_KEYS: Sequence[str] = tuple()
    """subkeys mandatory"""
    NEEDS: Sequence[str] = tuple()
    """Name of elements needed at the same level"""

    def is_multiple(self, data_to_parse: Any) -> bool:
        """Check if the corresponding data can be a list of item or a uniq element

        Parameters
        ----------
        data_to_parse: dict or list
            corresponding data to this key

        Returns
        -------
        True or False
        """
        return isinstance(data_to_parse, list)

    @property
    def keys(self) -> tuple[str, ...]:
        """All subkeys"""
        return tuple([*self.OPTIONAL_KEYS, *self.MANDATORY_KEYS])

    def get_data(self, data_to_parse: Any) -> Iterator[Any]:
        """yield data from parsable input data

        Parameters
        ----------
        data_to_parse

        Yields
        ------
        a unique items
        """
        data = self.load(data_to_parse)
        if self.is_multiple(data_to_parse):
            for d in data:
                yield d
        else:
            yield data

    def parse(self, data_to_parse: Any, **kwargs: Any) -> Any:
        data = self.load(data_to_parse)
        data = data.get(self.KEY)
        if not data and not self.OPTIONAL:
            raise EOTriggeringConfigurationError(f"mandatory section {self.KEY} is misconfigured")
        elif data:
            results, errors = [], []
            for data in self.get_data(data):
                step_result, step_errors = self._parse(data, **kwargs)
                errors.extend(step_errors)
                results.append(step_result)
            if errors:
                raise EOTriggeringConfigurationError("\n".join(errors))
            return results
        return self.DEFAULT

    @abstractmethod
    def _parse(self, data_to_parse: Any, **kwargs: Any) -> tuple[Any, list[str]]:
        """Specific parse method, use to parse a uniq item.

        In the case of multiple, is called one time per item

        Parameters
        ----------
        data_to_parse

        Returns
        -------
        tuple of result and errors.
        """

    def check_mandatory(self, data_to_parse: dict[str, Any]) -> list[str]:
        """Check if all mandatory key are present.

        Returns
        -------
        list of errors
        """
        return [f'missing "{key}" in "{self.KEY}" section' for key in self.MANDATORY_KEYS if key not in data_to_parse]

    def check_unknown(self, data_to_parse: dict[str, Any]) -> list[str]:
        """Check if only defined key are present

        Returns
        -------
        list of errors
        """
        return [f'Unkown {key=} in "{self.KEY} section"' for key in data_to_parse if key not in self.keys]


class EOProcessParser(ParseLoader):
    """Parser aggregator, use to parse multiple key for on payload data

    Parameters
    ----------
    *parsers: EOTriggeringKeyParser
        parsers to use

    """

    def __init__(self, *parsers: EOTriggeringKeyParser) -> None:
        self.parsers = {parser.KEY: parser for parser in parsers}

    def parse(self, data_to_parse: Any, result: dict[str, Any] = {}, **kwargs: Any) -> Any:
        result = {}
        for parse_name, parser in self.parsers.items():
            if parse_name not in result:
                if parser.NEEDS:
                    result |= EOProcessParser(*(self.parsers[key] for key in parser.NEEDS)).parse(
                        data_to_parse,
                        result=result,
                    )
                result[parse_name] = parser.parse(
                    data_to_parse, **{key: res for key, res in result.items() if key in parser.NEEDS}
                )
        return result
