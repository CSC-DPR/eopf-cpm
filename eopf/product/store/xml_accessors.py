from typing import TYPE_CHECKING, Any, Iterator, MutableMapping, Optional, TextIO

import lxml
import xarray as xr

from eopf.exceptions import StoreNotOpenError, XmlParsingError
from eopf.product.store import EOProductStore
from eopf.product.utils import (  # to be reviewed
    apply_xpath,
    parse_xml,
    translate_structure,
)

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_object import EOObject


class XMLAnglesAccessor(EOProductStore):
    def __init__(self, url: str, **kwargs: Any) -> None:
        super().__init__(url)
        self._root = lxml.etree._ElementTree
        self._url = url
        self.local_namespaces = {"n1": "https://psd-14.sentinel2.eo.esa.int/PSD/S2_PDI_Level-1C_Tile_Metadata.xsd"}

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open()
        self._root = lxml.etree.parse(self._url)

    def __getitem__(self, key: str) -> "EOObject":
        """
        This method is used to return eo_variables if parameters value match

        Parameters
        ----------
        key: str
            xpath

        Raise
        ----------
        AttributeError, it the given key doesn't match

        Return
        ----------
        EOVariable
        """
        from eopf.product.core import EOVariable

        variable_data = self.create_eo_variable(key)
        return EOVariable(data=variable_data)

    def create_eo_variable(self, xpath: str) -> xr.DataArray:
        """
        This method is used to recover and create datasets with angles values stored under
        <<VALUES>> tag.

        """
        item = self._root.xpath(xpath, namespaces=self.local_namespaces)[0]
        eo_variable_data = self.get_values(f"{self._root.getpath(item)}/VALUES")
        return eo_variable_data

    def get_values(self, path: str) -> xr.DataArray:
        """
        This method is used to convert data from a xml node to a xarray dataarray

        Parameters
        -------
        path: str
            xpath to node "Values_List" item

        Returns
        -------
        xr.DataArray
        """
        list = self._root.xpath(path, namespaces=self.local_namespaces)
        # Convert every value from xml to a floating point representation
        array = [[float(i) for i in x.text.split()] for x in list]
        # Create 2d DataArray
        da = xr.DataArray(array, dims=["y_tiepoints", "x_tiepoints"])
        return da

    def __iter__(self) -> Iterator[str]:
        """Has no functionality within this store"""
        yield from ()

    def __len__(self) -> int:
        """Has no functionality within this accessor"""
        return 0

    def __setitem__(self, key: str, value: Any) -> None:
        """Has no functionality within this store"""
        raise NotImplementedError()

    # docstr-coverage: inherited
    def is_group(self, path: str) -> bool:
        if "Values_List" in path:
            return False
        else:
            return len(self._root.xpath(path, namespaces=self.local_namespaces)[0].getchildren()) > 0

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        if "Values_List" in path:
            return True
        else:
            return len(self._root.xpath(path, namespaces=self.local_namespaces)[0].getchildren()) == 0

    def iter(self, path: str) -> Iterator[str]:
        """Has no functionality within this store"""
        yield from ()

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        raise NotImplementedError()


class XMLTPAccessor(EOProductStore):
    def __init__(self, url: str, dim: str, **kwargs: Any) -> None:
        self._root = lxml.etree._ElementTree
        self._url = url
        self._dim = dim[-1]  # x or y
        self.local_namespaces = {"n1": "https://psd-14.sentinel2.eo.esa.int/PSD/S2_PDI_Level-1C_Tile_Metadata.xsd"}

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        # Assure that given url is path to legacy product or path to MTD_TL.xml file
        self._root = lxml.etree.parse(self._url)

    def get_shape(self, xpath: str) -> list[int]:
        """
        This method is used to recover array shape from a given xpath node
        Parameters
        ----------
        xpath: str
            path to dimensions node

        Return
        ----------
        list(int):
            List with dimensions
        """
        list = self._root.xpath(xpath, namespaces=self.local_namespaces)
        return [len(list), len(list[0].text.split())]

    def tie_points_y(self) -> xr.DataArray:
        dom = self._root
        shape_y_x = self.get_shape("n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/Values_List/VALUES")
        ymax = float(
            dom.xpath(
                'n1:Geometric_Info/Tile_Geocoding/Geoposition[@resolution="10"]/ULY',
                namespaces=self.local_namespaces,
            )[0].text,
        )
        ystep = float(
            dom.xpath(
                "n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/ROW_STEP",
                namespaces=self.local_namespaces,
            )[0].text,
        )
        y = [ymax - i * ystep - ystep / 2 for i in range(shape_y_x[0])]
        return xr.DataArray(y)

    def tie_points_x(self) -> xr.DataArray:
        dom = self._root
        shape_y_x = self.get_shape("n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/Values_List/VALUES")
        xmin = float(
            dom.xpath(
                'n1:Geometric_Info/Tile_Geocoding/Geoposition[@resolution="10"]/ULX',
                namespaces=self.local_namespaces,
            )[0].text,
        )
        xstep = float(
            dom.xpath(
                "n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/COL_STEP",
                namespaces=self.local_namespaces,
            )[0].text,
        )
        x = [xmin + i * xstep + xstep / 2 for i in range(shape_y_x[1])]
        return xr.DataArray(x)

    def __getitem__(self, key: str) -> "EOObject":
        """
        This method is used to return eo_variables if parameter value match

        Parameters
        ----------
        key: str
            variable name

        Return
        ----------
        EOVariable, if the given key match

        Raise
        ----------
        AttributeError, it the given key doesn't match
        """
        from eopf.product.core import EOVariable

        if self._dim == "y" and key == "y":
            eo_variable_data = self.tie_points_y()
        elif self._dim == "x" and key == "x":
            eo_variable_data = self.tie_points_x()
        else:
            raise NotImplementedError("Invalid dimension")
        return EOVariable(name=key, data=eo_variable_data)

    def __iter__(self) -> Iterator[str]:
        yield from ()

    def __len__(self) -> int:
        """Has no functionality within this store"""
        raise NotImplementedError()

    def __setitem__(self, key: str, value: Any) -> None:
        """Has no functionality within this store"""
        raise NotImplementedError()

    def is_group(self, path: str) -> bool:
        # xmlTP accessors only returns variables
        return False

    def is_variable(self, path: str) -> bool:
        return True

    def iter(self, path: str) -> Iterator[str]:
        """Has no functionality within this store"""
        yield from ()

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        raise NotImplementedError()


class XMLManifestAccessor(EOProductStore):
    KEYS = ["CF", "OM_EOP"]

    def __init__(self, url: str, **kwargs: Any) -> None:
        self._attrs: MutableMapping[str, Any] = {}
        for key in self.KEYS:
            self._attrs[key] = {}
        self._parsed_xml = None
        self._url = url
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
            self._metada_mapping: MutableMapping[str, Any] = kwargs["metadata_mapping"]
            self._namespaces: dict[str, str] = kwargs["namespaces"]
        except KeyError as e:
            raise TypeError(f"Missing configuration parameter: {e}")

        # open the manifest xml
        self._xml_fobj = open(self._url, mode="r")

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
        from eopf.product.core import EOGroup

        eog: EOGroup = EOGroup("product_metadata", attrs=self._attrs)
        return eog

    def __iter__(self) -> Iterator[str]:
        """Iterator over the dict containing the CF and OM_EOP attributes of the EOProduct

        Returns
        ----------
        Iterator[str]:
            An iterator over self.KEYS
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

    def __len__(self) -> int:
        """Has no functionality within this store"""
        raise NotImplementedError()

    def __setitem__(self, key: str, value: "EOObject") -> None:
        raise NotImplementedError()

    def is_group(self, path: str) -> bool:
        raise NotImplementedError()

    def is_variable(self, path: str) -> bool:
        raise NotImplementedError()

    def iter(self, path: str) -> Iterator[str]:
        """Has no functionality within this store"""
        raise NotImplementedError()

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        raise NotImplementedError()
