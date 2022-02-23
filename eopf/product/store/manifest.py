import os
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterator, MutableMapping

from eopf.product.conveniences import (
    apply_xpath,
    etree_to_dict,
    parse_xml,
    translate_structure,
)
from eopf.product.store import EOProductStore


class ManifestStore(EOProductStore):

    KEYS = ["CF", "OM_EOP", "XFDU"]

    def __init__(self, url: str) -> None:
        super().__init__(url)
        self._attrs = {}
        for key in self.KEYS:
            self._attrs[key] = {}
        self._metada_mapping = {}
        self._namespaces: Dict[str, str] = {}
        self._xfdu_dom = None

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        if os.path.isfile(self.url):
            self._xml_fobj = open(self.url, mode="r", **kwargs)
            super().open()
        else:
            raise (f"No XML file at: {self.url} ")

    def close(self) -> None:
        self._xml_fobj.close()
        super().close()

    def is_group(self, path: str) -> bool:
        raise NotImplementedError()

    def is_variable(self, path: str) -> bool:
        raise NotImplementedError()

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        raise NotImplementedError()

    def iter(self, path: str) -> Iterator[str]:
        if path in self.KEYS:
            return self._attrs[path].keys()
        else:
            raise KeyError(f"Invalid path: {path}; see KEYS")

    def __getitem__(self, key: str) -> MutableMapping[str, Any]:
        if key in self.KEYS:
            if key == "OM_EOP":
                self._get_om_eop()
            elif key == "CF":
                self._get_cf()
            else:
                # key == "XFDU":
                self._get_xfdu()
            return self._attrs[key]
        else:
            raise KeyError(f"Invalid key: {key}; see KEYS")

    def __setitem__(self, key: str, value: Any) -> None:
        if key == "metadata_mapping":
            self._metada_mapping = value
        elif key == "namespaces":
            self._namespaces = value
        else:
            raise KeyError(f"Invalid key: {key}; valid options: metadata_mapping, namespaces;")

    def __len__(self) -> int:
        return len(self._attrs)

    def __iter__(self) -> Iterator[str]:
        for key in self.KEYS:
            return self._attrs[key]

    def _get_xfdu(self):
        try:
            tree = ET.parse(self.url)
            root = tree.getroot()
            xfdu = etree_to_dict(root[1])
            self._attrs["XFDU"] = xfdu
        except Exception as e:
            raise (f"Exception while computing xfdu: {e}")

    def _get_xfdu_dom(self) -> None:
        try:
            self._xfdu_dom = parse_xml(self._xml_fobj)
        except Exception as e:
            raise (f"Exception while computing xfdu dom: {e}")

    def _get_cf(self):
        if not self._xfdu_dom:
            self._get_xfdu_dom()
        cf = {
            attr: apply_xpath(self._xfdu_dom, self._metada_mapping["CF"][attr], self._namespaces)
            for attr in self._metada_mapping["CF"]
        }
        self._attrs["CF"] = cf

    def _get_om_eop(self):
        if not self._xfdu_dom:
            self._get_xfdu_dom()
        eop = {
            attr: translate_structure(self._metada_mapping["OM_EOP"][attr], self._xfdu_dom, self._namespaces)
            for attr in self._metada_mapping["OM_EOP"]
        }
        self._attrs["OM_EOP"] = eop
