from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterator

from eopf.product.store import EOProductStore
from eopf.product.store.xml_accessors import (
    XMLAnglesAccessor,
    XMLManifestAccessor,
    XMLTPAccessor,
)

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_object import EOObject


class XMLAccessor(EOProductStore):
    class_handler = {
        "xmlangles": XMLAnglesAccessor,
        "xmltpx": XMLTPAccessor,
        "xmltpy": XMLTPAccessor,
        "manifest": XMLManifestAccessor,
    }

    def __init__(self, url: str, type_: str) -> None:
        super().__init__(url)
        self._url = url
        self.inner_xml_accessor: Any
        # Create proxy object based on class selector
        if type_ in XMLAccessor.class_handler.keys():
            self.inner_xml_accessor = XMLAccessor.class_handler[type_](url=url, type=type_)

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        if hasattr(self.inner_xml_accessor, "open"):
            self.inner_xml_accessor.open(**kwargs)
            super().open(**kwargs)

    def __getitem__(self, item: str) -> None:
        if hasattr(self.inner_xml_accessor, "__getitem__"):
            return self.inner_xml_accessor[item]

    def __iter__(self) -> Iterator[str]:
        if hasattr(self.inner_xml_accessor, "__iter__"):
            return iter(self.inner_xml_accessor)
        raise NotImplementedError()

    def __len__(self) -> int:
        if hasattr(self.inner_xml_accessor, "__len__"):
            return len(self.inner_xml_accessor)
        raise NotImplementedError()

    def __setitem__(self, key: str, value: "EOObject") -> None:
        if hasattr(self.inner_xml_accessor, "__setitem__"):
            self.inner_xml_accessor[key] = value
            return
        raise NotImplementedError()

    def is_group(self, path: str) -> bool:
        if hasattr(self.inner_xml_accessor, "is_group"):
            return self.inner_xml_accessor.is_group(path)
        raise NotImplementedError()

    def is_variable(self, path: str) -> bool:
        if hasattr(self.inner_xml_accessor, "is_variable"):
            return self.inner_xml_accessor.is_variable(path)
        raise NotImplementedError()

    def iter(self, path: str) -> Iterator[str]:
        if hasattr(self.inner_xml_accessor, "iter"):
            return self.inner_xml_accessor.iter(path)
        raise NotImplementedError()

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        if hasattr(self.inner_xml_accessor, "write_attrs"):
            return self.inner_xml_accessor.write_attrs()
        raise NotImplementedError()
