from typing import Any, MutableMapping, Optional, TextIO, TYPE_CHECKING
import lxml
import xarray as xr
import os
from eopf.exceptions import (
    MissingConfigurationParameter,
    StoreNotOpenError,
    XmlParsingError,
)
from eopf.product.utils import apply_xpath, parse_xml, translate_structure # to be reviewed
if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_object import EOObject


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


class XMLAnglesAccessor:
    def __init__(self, **kwargs: Any) -> None:
        self._root = lxml.etree._ElementTree
        self._url = kwargs["url"]
        self.local_namespaces = {"n1": "https://psd-14.sentinel2.eo.esa.int/PSD/S2_PDI_Level-1C_Tile_Metadata.xsd"}

    def open(self) -> None:
        if "MTD_TL.xml" not in self._url:
            xml_url = get_mtd_file_path(self._url)
            self._root = lxml.etree.parse(xml_url)
        else:
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
        variable_name, variable_data = self.create_eo_variable(key)
        return EOVariable(name=variable_name, data=variable_data)

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


class XMLTPAccessor:
    def __init__(self, **kwargs: Any) -> None:
        self._root = lxml.etree._ElementTree
        self._url = kwargs["url"]
        self._dim = kwargs["type"][-1]  # x or y
        self.local_namespaces = {"n1": "https://psd-14.sentinel2.eo.esa.int/PSD/S2_PDI_Level-1C_Tile_Metadata.xsd"}

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        # Assure that given url is path to legacy product or path to MTD_TL.xml file
        if "MTD_TL.xml" not in self._url:
            xml_url = get_mtd_file_path(self._url)
            self._root = lxml.etree.parse(xml_url)
        else:
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

    def tie_points(self, dimension: str) -> xr.DataArray:
        dom = self._root
        shape_y_x = self.get_shape("n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/Values_List/VALUES")
        # To verify if other resolutions values should be read
        ymax = float(dom.xpath('n1:Geometric_Info/Tile_Geocoding/Geoposition[@resolution="10"]/ULY',
                               namespaces=self.local_namespaces, )[0].text, )
        xmin = float(dom.xpath('n1:Geometric_Info/Tile_Geocoding/Geoposition[@resolution="10"]/ULX',
                               namespaces=self.local_namespaces, )[0].text, )
        ystep = float(dom.xpath("n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/ROW_STEP",
                                namespaces=self.local_namespaces, )[0].text, )
        xstep = float(dom.xpath("n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/COL_STEP",
                                namespaces=self.local_namespaces, )[0].text, )

        y = [ymax - i * ystep - ystep / 2 for i in range(shape_y_x[0])]
        x = [xmin + i * xstep + xstep / 2 for i in range(shape_y_x[1])]
        dataset = xr.Dataset({"y": y, "x": x})
        return dataset[dimension]

    def tie_points_y(self) -> xr.DataArray:
        dom = self._root
        shape_y_x = self.get_shape("n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/Values_List/VALUES")
        ymax = float(dom.xpath('n1:Geometric_Info/Tile_Geocoding/Geoposition[@resolution="10"]/ULY',
                               namespaces=self.local_namespaces, )[0].text, )
        ystep = float(dom.xpath("n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/ROW_STEP",
                                namespaces=self.local_namespaces, )[0].text, )
        y = [ymax - i * ystep - ystep / 2 for i in range(shape_y_x[0])]
        return xr.DataArray(y)

    def tie_points_x(self) -> xr.DataArray:
        dom = self._root
        shape_y_x = self.get_shape("n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/Values_List/VALUES")
        xmin = float(dom.xpath('n1:Geometric_Info/Tile_Geocoding/Geoposition[@resolution="10"]/ULX',
                               namespaces=self.local_namespaces, )[0].text, )
        xstep = float(dom.xpath("n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/COL_STEP",
                                namespaces=self.local_namespaces, )[0].text, )
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


class XMLManifestAccessor:
    KEYS = ["CF", "OM_EOP"]

    def __init__(self, **kwargs: Any) -> None:
        self._attrs: MutableMapping[str, Any] = {}
        for key in self.KEYS:
            self._attrs[key] = {}
        self._parsed_xml = None
        self._url = kwargs["url"]
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
            if not isinstance(config_dict, dict):
                raise MissingConfigurationParameter(" The parameter: config; should be a dictionary")
            self._metada_mapping: MutableMapping[str, Any] = config_dict["metadata_mapping"]
            self._namespaces: dict[str, str] = config_dict["namespaces"]
        except KeyError as e:
            raise KeyError(f"Missing configuration pameter: {e}")

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
