import os
import pathlib
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Dict, Iterator

import lxml
import xarray as xr

from eopf.exceptions import StoreNotOpenError
from eopf.product.store import EOProductStore

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_object import EOObject


class EOXmlAnglesAccesor(EOProductStore):
    """
    Accesor class used to read and compute Angles data from an given url

    Parameters
    ----------
    url: str
        path url or the target store

    """

    RESTRICTED_ATTR_KEY = ("_FillValue",)

    def __init__(self, url: str) -> None:
        super().__init__(url)
        self._root = lxml.etree._ElementTree
        self._url = url
        self.eo_variables: Dict[str, xr.DataArray] = dict()
        self.local_namespaces = {"n1": "https://psd-14.sentinel2.eo.esa.int/PSD/S2_PDI_Level-1C_Tile_Metadata.xsd"}

    # docstr-coverage: inherited
    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open()
        # Assure that given url is path to legacy product or path to MTD_TL.xml file
        if "MTD_TL.xml" not in self._url:
            xml_url = EOXmlAnglesAccesor.get_mtd_file_path(self._url)
            self._root = lxml.etree.parse(xml_url)
        else:
            self._root = lxml.etree.parse(self._url)

    # docstr-coverage: inherited
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
        EOVariabl
        """

        from eopf.product.core import EOVariable

        variable_name, variable_data = self.create_eo_variable(key)
        return EOVariable(name=variable_name, data=variable_data)

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        raise NotImplementedError()

    # docstr-coverage: inherited
    def iter(self, path: str) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return iter([])

    # docstr-coverage: inherited
    def __setitem__(self, key: str, value: "EOObject") -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        raise NotImplementedError()

    def __len__(self) -> int:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        raise NotImplementedError()

    # docstr-coverage: inherited
    def __iter__(self) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return iter([])

    @staticmethod
    def get_mtd_file_path(path: str) -> str:
        """
        This static method is used to locate path of MTD_TL.xml file
        from a given legacy product path.

        Parameters
        -------
        path: str
            path to legacy product

        Returns
        -------
        str: path to target file
        """
        fpath = os.listdir(f"{path}/GRANULE/")[1]
        return f"{path}/GRANULE/{fpath}/MTD_TL.xml"

    def create_eo_variable(self, xpath: str) -> tuple[str, xr.DataArray]:
        """
        This method is used to recover and create datasets with angles values stored under
        <<VALUES>> tag.

        """
        item = self._root.xpath(xpath, namespaces=self.local_namespaces)[0]
        var_name = self.get_variable_name(self._root.getpath(item))
        eo_variable_data = self.get_values(f"{self._root.getpath(item)}/VALUES")
        dataset = xr.Dataset({var_name: eo_variable_data})
        return (var_name, dataset[var_name])

    def is_group(self, path: str) -> bool:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        raise NotImplementedError()

    def is_variable(self, path: str) -> bool:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        raise NotImplementedError()

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

    def get_variable_name(self, path: str) -> str:
        """
        This method is used to get a varible name from a given path

        Parameters
        -------
        path: str
            xpath to node item

        Returns
        -------
        str, variable name
        """
        if "Sun_Angles_Grid" in path.split("/"):
            # Return <<SAA>> Sun Azimuth Angles or <<SZA>> Sun Zenit Angles if node parent is <<Sun_Angles_Grid>>
            tile_path = "/".join(path.split("/")[0:5])
            node_path = self._root.xpath(tile_path, namespaces=self.local_namespaces)
            return "saa" if "Azimuth" in node_path[0].tag else "sza"
        else:
            # Recover band and detector ID, and compose variable name
            tile_path = "/".join(path.split("/")[0:5])
            node_path = self._root.xpath(tile_path, namespaces=self.local_namespaces)
            item = node_path[0]
            if item.tag == "Viewing_Incidence_Angles_Grids":
                if "Azimuth" in path.split("/"):
                    return f"vaa_b{item.attrib['bandId']}_{item.attrib['detectorId']}"
                else:
                    return f"vza_b{item.attrib['bandId']}_{item.attrib['detectorId']}"
        # Return a dummy name if node is neither Sun_Angles_Grid nor Viewing_Incidence_Angles_Grids
        return "var_name"


class EOXmltpAccessor(EOProductStore):
    """
    Accesor class used to read and compute tie points data from an given url

    Parameters
    ----------
    url: str
        path url or the target store

    """

    def __init__(self, url: str) -> None:
        super().__init__(url)
        self._url = url
        self._root = lxml.etree._ElementTree
        self.tie_points: Dict[str, xr.DataArray] = dict()
        self.local_namespaces = {"n1": "https://psd-14.sentinel2.eo.esa.int/PSD/S2_PDI_Level-1C_Tile_Metadata.xsd"}

    def get_tiepoints_ds(self) -> None:
        dom = self._root
        shape_y_x = self.get_shape("n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/Values_List/VALUES")
        # To verify if other resolutions values should be read
        ymax = float(
            dom.xpath(
                'n1:Geometric_Info/Tile_Geocoding/Geoposition[@resolution="10"]/ULY',
                namespaces=self.local_namespaces,
            )[0].text,
        )
        xmin = float(
            dom.xpath(
                'n1:Geometric_Info/Tile_Geocoding/Geoposition[@resolution="10"]/ULX',
                namespaces=self.local_namespaces,
            )[0].text,
        )
        ystep = float(
            dom.xpath(
                "n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/ROW_STEP",
                namespaces=self.local_namespaces,
            )[0].text,
        )
        xstep = float(
            dom.xpath(
                "n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/COL_STEP",
                namespaces=self.local_namespaces,
            )[0].text,
        )

        y = [ymax - i * ystep - ystep / 2 for i in range(shape_y_x[0])]
        x = [xmin + i * xstep + xstep / 2 for i in range(shape_y_x[1])]
        dataset = xr.Dataset({"y": y, "x": x})
        self.tie_points = {var_name: dataset[var_name] for var_name in dataset.variables}

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

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open()
        # Assure that given url is path to legacy product or path to MTD_TL.xml file
        if "MTD_TL.xml" not in self._url:
            xml_url = EOXmlAnglesAccesor.get_mtd_file_path(self._url)
            self._root = lxml.etree.parse(xml_url)
        else:
            self._root = lxml.etree.parse(self._url)
        self.get_tiepoints_ds()

    def close(self) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        super().close()
        self._root.close()
        self._root = None

    def is_group(self, path: str) -> bool:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        raise NotImplementedError()

    def is_variable(self, path: str) -> bool:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        raise NotImplementedError()

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        raise NotImplementedError()

    # docstr-coverage: inherited
    def iter(self, path: str) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return iter(self.tie_points.keys())

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
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        from eopf.product.core import EOVariable

        if key not in self.tie_points.keys():
            raise AttributeError(f"{key} not found!")
        return EOVariable(key, data=self.tie_points[key])

    def __setitem__(self, key: str, value: "EOObject") -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        raise NotImplementedError()

    def __len__(self) -> int:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        raise NotImplementedError()

    # docstr-coverage: inherited
    def __iter__(self) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return iter(self.tie_points.keys())

    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return pathlib.Path(file_path).suffix in [".xml"]
