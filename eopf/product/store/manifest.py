from typing import Any, Dict, Iterator, MutableMapping, Optional, TextIO

from eopf.exceptions import (
    MissingConfigurationParameter,
    StoreNotOpenError,
    XmlParsingError,
)
from eopf.product.conveniences import apply_xpath, parse_xml, translate_structure
from eopf.product.core import EOGroup
from eopf.product.store import EOProductStore


class ManifestStore(EOProductStore):
    """Store representation to access and retrieve EOProduct attributes,
    used by EOSafeStore to convert legacy formats into EOProducts

    Parameters
    ----------
    url: str
        file path to the legacy format manifest xml

    Attributes
    ----------
    KEYS: list[str]
        the keys defininf the types of EOProduct attributes
    """

    KEYS = ["CF", "OM_EOP"]

    def __init__(self, url: str) -> None:
        """Instantiate based on the url(file path) of the manifest xml file

        Parameters
        ----------
        url: str,
            file path to the manifest xml file
        """
        super().__init__(url)
        self._attrs: MutableMapping[str, Any] = {}
        for key in self.KEYS:
            self._attrs[key] = {}
        self._parsed_xml = None
        self._xml_fobj: Optional[TextIO] = None

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        """Open the store, the xml file and set the necessary configuration parameters

        Parameters
        ----------
        mode: str, optional
            unused
        **kwargs: Any
            extra kwargs through which configuration parameters are passsed
        """
        if mode != "r":
            raise NotImplementedError()

        # get configuration parameters
        if "config" not in kwargs.keys():
            raise MissingConfigurationParameter(" The parameter: config; is missing")
        try:
            config_dict: Optional[Any] = kwargs.get("config")
            if not isinstance(config_dict, Dict):
                raise MissingConfigurationParameter(" The parameter: config; should be a dictionary")
            self._metada_mapping: MutableMapping[str, Any] = config_dict["metadata_mapping"]
            self._namespaces: Dict[str, str] = config_dict["namespaces"]
        except KeyError as e:
            raise KeyError(f"Missing configuration pameter: {e}")

        # open the manifest xml
        self._xml_fobj = open(self.url, mode="r")

        # open the store
        super().open(mode, **kwargs)

    def close(self) -> None:
        """Closes the store and the xml file object"""
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")
        self._xml_fobj.close()
        super().close()

    def is_group(self, path: str) -> bool:
        """Has no functionality within this store"""
        raise NotImplementedError()

    def is_variable(self, path: str) -> bool:
        """Has no functionality within this store"""
        raise NotImplementedError()

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        """Has no functionality within this store"""
        raise NotImplementedError()

    def iter(self, path: str) -> Iterator[str]:
        """Has no functionality within this store"""
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")

        if path not in ["", self.sep]:
            raise KeyError(f"Invalid path: {path}")

        yield from ()

    def __getitem__(self, key: str) -> MutableMapping[str, Any]:
        """Getter for CF and OM_EOP attributes of the EOProduct

        Parameters
        ----------
        key: str, optional
            unused
        Returns
        ----------
        The CF an OM_EOP dictionary as attributes of a EOGroup: MutableMapping[str, Any]:
        """
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")

        # computes CF and OM_EOP
        if self._parsed_xml is None:
            self._parse_xml()
        self._compute_om_eop()
        self._compute_cf()

        # create an EOGroup and set its attributes with a dictionary containing CF and OM_EOP
        eog: EOGroup = EOGroup("product_metadata", attrs=self._attrs)
        return eog

    def __setitem__(self, key: str, value: Any) -> None:
        """Has no functionality within this store"""
        raise NotImplementedError()

    def __len__(self) -> int:
        """Has no functionality within this store"""
        return 0

    def __iter__(self) -> Iterator[str]:
        """Iterator over the dict containing the CF and OM_EOP attributes of the EOProduct

        Returns
        ----------
        An iterator over self.KEYS : Iterator[str]
        """
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")

        yield from self.KEYS

    def _parse_xml(self) -> None:
        """Parses the manifest xml and saves it in _parsed_xml

        Raises
        ----------
        StoreNotOpenError
            Trying to parse an xml that was not opened
        XmlParsingError
            Any error while parsing the xml
        """
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")

        try:
            self._parsed_xml = parse_xml(self._xml_fobj)
        except Exception as e:
            raise XmlParsingError(f"Exception while computing xfdu dom: {e}")

    def _compute_cf(self) -> None:
        """Computes the CF dictionary of attributes and saves it in _metada_mapping

        Raises
        ----------
        StoreNotOpenError
            Trying to compute CF from an opened that was not opened
        XmlParsingError
            Any error while applying xpath
        """
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")

        try:
            cf = {
                attr: apply_xpath(self._parsed_xml, self._metada_mapping["CF"][attr], self._namespaces)
                for attr in self._metada_mapping["CF"]
            }
            self._attrs["CF"] = cf
        except Exception as e:
            raise XmlParsingError(f"Exception while computing CF: {e}")

    def _compute_om_eop(self) -> None:
        """Computes the OM_EOP dictionary of attributes and saves it in _metada_mapping

        Raises
        ----------
        StoreNotOpenError
            Trying to compute CF from an opened that was not opened
        XmlParsingError
            Any error while from translate_structure
        """
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")

        try:
            eop = {
                attr: translate_structure(self._metada_mapping["OM_EOP"][attr], self._parsed_xml, self._namespaces)
                for attr in self._metada_mapping["OM_EOP"]
            }
            self._attrs["OM_EOP"] = eop
        except Exception as e:
            raise XmlParsingError(f"Exception while computing OM_EOP: {e}")
