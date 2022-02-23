import os
from typing import Any, Dict, Iterator, MutableMapping, Optional, TextIO

from eopf.exceptions import (
    FileNotExists,
    FileOpenError,
    MissingConfigurationParameter,
    StoreNotOpenError,
    XmlParsingError,
)
from eopf.product.conveniences import apply_xpath, parse_xml, translate_structure
from eopf.product.core import EOGroup
from eopf.product.store import EOProductStore


class ManifestStore(EOProductStore):

    KEYS = ["CF", "OM_EOP"]

    def __init__(self, url: str) -> None:
        super().__init__(url)
        self._attrs: MutableMapping[str, Any] = {}
        for key in self.KEYS:
            self._attrs[key] = {}
        self._xfdu_dom = None
        self._xml_fobj: Optional[TextIO] = None

    def open(self, mode: str = "r", **kwargs: Any) -> None:

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
        if os.path.isfile(self.url):
            try:
                self._xml_fobj = open(self.url, mode="r")
            except Exception as e:
                raise FileOpenError(f"Error when open file {self.url}: {e}")
            super().open()
        else:
            raise FileNotExists(f"No XML file at: {self.url} ")

    def close(self) -> None:
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")
        self._xml_fobj.close()
        super().close()

    def is_group(self, path: str) -> bool:
        raise NotImplementedError()

    def is_variable(self, path: str) -> bool:
        raise NotImplementedError()

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        raise NotImplementedError()

    def iter(self, path: str) -> Iterator[str]:
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")

        if path not in ["", super().sep]:
            raise KeyError(f"Invalid path: {path}")

        yield ""

    def __getitem__(self, key: str) -> MutableMapping[str, Any]:
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")

        self._get_xfdu_dom()
        self._get_om_eop()
        self._get_cf()
        eog: EOGroup = EOGroup("product_metadata", attrs=self._attrs)
        return eog

    def __setitem__(self, key: str, value: Any) -> None:
        raise NotImplementedError()

    def __len__(self) -> int:
        return 0

    def __iter__(self) -> Iterator[str]:
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")

        for key in self.KEYS:
            yield self._attrs[key]

    def _get_xfdu_dom(self) -> None:
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")

        try:
            self._xfdu_dom = parse_xml(self._xml_fobj)
        except Exception as e:
            raise XmlParsingError(f"Exception while computing xfdu dom: {e}")

    def _get_cf(self) -> None:
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")

        try:
            cf = {
                attr: apply_xpath(self._xfdu_dom, self._metada_mapping["CF"][attr], self._namespaces)
                for attr in self._metada_mapping["CF"]
            }
            self._attrs["CF"] = cf
        except Exception as e:
            raise XmlParsingError(f"Exception while computing CF: {e}")

    def _get_om_eop(self) -> None:
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")

        try:
            eop = {
                attr: translate_structure(self._metada_mapping["OM_EOP"][attr], self._xfdu_dom, self._namespaces)
                for attr in self._metada_mapping["OM_EOP"]
            }
            self._attrs["OM_EOP"] = eop
        except Exception as e:
            raise XmlParsingError(f"Exception while computing OM_EOP: {e}")
