from typing import TYPE_CHECKING, Any, Iterator, MutableMapping, Optional, TextIO

from eopf.exceptions import StoreNotOpenError, XmlParsingError
from eopf.product.core import EOGroup
from eopf.product.store import EOProductStore
from eopf.product.utils import parse_xml, translate_structure

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_object import EOObject


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
        the keys defining the types of EOProduct attributes
    """

    def __init__(self, url: str) -> None:
        super().__init__(url)
        self._attrs: MutableMapping[str, Any] = {}
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
        try:
            self._metada_mapping: MutableMapping[str, Any] = kwargs["mapping"]
            self._namespaces: dict[str, str] = kwargs["namespaces"]
        except KeyError as e:
            raise TypeError(f"Missing configuration pameter: {e}")

        # open the manifest xml
        self._xml_fobj = open(self.url, mode="r")
        # open the store
        super().open(mode, **kwargs)

        # computes CF and OM_EOP
        if self._parsed_xml is None:
            self._parse_xml()

        for attr_mapping in self._metada_mapping:
            self._compute_attr(attr_mapping)

    def close(self) -> None:
        """Closes the store and the xml file object"""
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")
        self._xml_fobj.close()
        super().close()

    def is_group(self, path: str) -> bool:
        """Has no functionality within this store"""
        if path in ["", "/"]:
            return True
        raise NotImplementedError()

    def is_variable(self, path: str) -> bool:
        """Has no functionality within this store"""
        if path in ["", "/"]:
            return False
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

    def __getitem__(self, key: str) -> "EOObject":
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

        if key not in ["", self.sep]:
            raise KeyError(f"Invalid path: {key}")

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
        Iterator[str]:
            An iterator over self.KEYS
        """
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")

        return iter([])

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

    def _compute_attr(self, key: str) -> None:
        """Computes the key dictionary of attributes and saves it in _attrs

        Raises
        ----------
        StoreNotOpenError
            Trying to compute CF from an opened that was not opened
        XmlParsingError
            Any error while from translate_structure
        """
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")
        # try:
        self._attrs[key] = translate_structure(self._metada_mapping[key], self._parsed_xml, self._namespaces)
        # except Exception as e:
        #    raise XmlParsingError(f"Exception while computing {key}: {e}")
