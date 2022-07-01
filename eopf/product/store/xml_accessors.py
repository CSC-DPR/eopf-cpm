from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    MutableMapping,
    Optional,
    TextIO,
    Union,
)

import lxml
import numpy as np
import xarray as xr
from lxml.etree import _ElementUnicodeResult

from eopf.exceptions import StoreNotOpenError, XmlManifestNetCDFError, XmlParsingError
from eopf.formatting import EOFormatterFactory
from eopf.formatting.formatters import IsOptional, Text, ToImageSize
from eopf.product.core.eo_variable import EOVariable
from eopf.product.store import EONetCDFStore, EOProductStore, StorageStatus
from eopf.product.utils import apply_xpath, parse_xml  # to be reviewed

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_object import EOObject


class XMLAnglesAccessor(EOProductStore):
    def __init__(self, url: str, **kwargs: Any) -> None:
        super().__init__(url)
        self._root = lxml.etree._ElementTree

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        if mode != "r":
            raise NotImplementedError()

        super().open()

        # Recover configuration
        self._root = lxml.etree.parse(self.url)
        try:
            self._namespaces: dict[str, str] = kwargs["namespace"]
        except KeyError as e:
            raise TypeError(f"Missing configuration parameter: {e}")

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
        from eopf.formatting import EOFormatterFactory
        from eopf.product.core import EOVariable

        if self.status != StorageStatus.OPEN:
            raise StoreNotOpenError()

        formatter_name, formatter, xpath = EOFormatterFactory().get_formatter(key)
        xpath_result = self._root.xpath(xpath, namespaces=self._namespaces)
        if len(xpath_result) == 0:
            raise KeyError(f"invalid xml xpath : {key}")
        if formatter_name is not None and formatter is not None:
            return EOVariable(data=formatter(xpath_result))
        else:
            return EOVariable(data=self.create_eo_variable(xpath_result))

    def create_eo_variable(self, xpath_result: List[lxml.etree._Element]) -> xr.DataArray:
        """
        This method is used to recover and create datasets with angles values stored under
        <<VALUES>> tag.

        """
        if len(xpath_result) == 1:
            return self.get_values(f"{self._root.getpath(xpath_result[0])}/VALUES")
        else:
            array_order = []
            max_detector_id = -1
            max_shape: Any = (-1, -1)
            # Recover informations about each element in xpath's output (bandId, detectorId, data)
            for element in xpath_result:
                # Get bandId and detectorID from parentID (i.e azimuth / zenith)
                parent_node = element.getparent().getparent().attrib
                band, detector = map(int, parent_node.values())
                # Compute maximum detectorId
                if detector > max_detector_id:
                    max_detector_id = detector
                data = self.get_values(f"{self._root.getpath(element)}/VALUES")
                # Compute maximum data shape
                if data.shape > max_shape:
                    max_shape = data.shape
                array_order.append((band, detector, data))
            # Map data to 4d array
            zs = np.zeros(shape=(len(array_order), max_detector_id + 1, *max_shape))
            for array_parser in array_order:
                zs[array_parser[0]][array_parser[1]] = array_parser[2]
            return xr.DataArray(zs, dims=["bands", "detectors", "y_tiepoints", "x_tiepoints"])

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
        list = self._root.xpath(path, namespaces=self._namespaces)
        # Convert every value from xml to a floating point representation
        array = [[float(i) for i in x.text.split()] for x in list]
        # Create 2d DataArray
        da = xr.DataArray(array, dims=["y_tiepoints", "x_tiepoints"])
        return da

    def __iter__(self) -> Iterator[str]:
        """Has no functionality within this store"""
        if self.status != StorageStatus.OPEN:
            raise StoreNotOpenError()
        yield from ()

    def __len__(self) -> int:
        """Has no functionality within this accessor"""
        if self.status != StorageStatus.OPEN:
            raise StoreNotOpenError()
        return 0

    def __setitem__(self, key: str, value: Any) -> None:
        """Has no functionality within this store"""
        raise NotImplementedError()

    # docstr-coverage: inherited
    def is_group(self, path: str) -> bool:
        if self.status != StorageStatus.OPEN:
            raise StoreNotOpenError()
        if path.endswith("Values_List"):
            return False
        nodes_matched = self._root.xpath(path, namespaces=self._namespaces)
        return len(nodes_matched) == 1

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        if self.status != StorageStatus.OPEN:
            raise StoreNotOpenError()
        if not path.endswith("Values_List"):
            return False
        nodes_matched = self._root.xpath(path, namespaces=self._namespaces)
        return len(nodes_matched) == 1

    def iter(self, path: str) -> Iterator[str]:
        """Has no functionality within this store"""
        if self.status != StorageStatus.OPEN:
            raise StoreNotOpenError()
        yield from ()

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        raise NotImplementedError()


class XMLTPAccessor(EOProductStore):
    def __init__(self, url: str, **kwargs: Any) -> None:
        super().__init__(url)
        self._root = lxml.etree._ElementTree

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        # Assure that given url is path to legacy product or path to MTD_TL.xml file

        try:
            self._xmltp_step: Any = kwargs["step_path"]  # replace Any with actual type (typing issue)
            self._xmltp_value: Any = kwargs["values_path"]
            self._namespaces: Any = kwargs["namespace"]
        except KeyError as e:
            raise TypeError(f"Missing configuration parameter: {e}")
        self._root = lxml.etree.parse(self.url)
        super().open()

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
        list_ = self._root.xpath(xpath, namespaces=self._namespaces)
        return [len(list_), len(list_[0].text.split())]

    def get_tie_points_data(self, path: str) -> xr.DataArray:
        dom = self._root
        shape_y_x = self.get_shape(self._xmltp_value)
        resolution = float(dom.xpath(path, namespaces=self._namespaces)[0].text)
        step = float(dom.xpath(self._xmltp_step, namespaces=self._namespaces)[0].text)
        if path[-1] == "Y":
            data = [resolution - i * step - step / 2 for i in range(shape_y_x[0])]
        elif path[-1] == "X":
            data = [resolution + i * step + step / 2 for i in range(shape_y_x[1])]
        else:
            raise AttributeError("Invalid dimension")
        return xr.DataArray(data)

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

        if self.status != StorageStatus.OPEN:
            raise StoreNotOpenError()
        if not len(self._root.xpath(key, namespaces=self._namespaces)):
            raise TypeError(f"Incorrect xpath {key}")

        return EOVariable(name=key, data=self.get_tie_points_data(key))

    def __iter__(self) -> Iterator[str]:
        if self.status != StorageStatus.OPEN:
            raise StoreNotOpenError()
        raise NotImplementedError()

    def __len__(self) -> int:
        if self.status != StorageStatus.OPEN:
            raise StoreNotOpenError()
        """Has no functionality within this store"""
        return 2

    def __setitem__(self, key: str, value: Any) -> None:
        """Has no functionality within this store"""
        raise NotImplementedError()

    def is_group(self, path: str) -> bool:
        if self.status != StorageStatus.OPEN:
            raise StoreNotOpenError()
        # xmlTP accessors only returns variables
        return False

    def is_variable(self, path: str) -> bool:
        if self.status != StorageStatus.OPEN:
            raise StoreNotOpenError()
        return len(self._root.xpath(path, namespaces=self._namespaces)) == 1

    def iter(self, path: str) -> Iterator[str]:
        """Has no functionality within this store"""
        if self.status != StorageStatus.OPEN:
            raise StoreNotOpenError()
        yield from ()

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        raise NotImplementedError()


class XMLManifestAccessor(EOProductStore):
    KEYS = ["CF", "OM_EOP"]

    def __init__(self, url: str, **kwargs: Any) -> None:
        super().__init__(url)
        self._attrs: MutableMapping[str, Any] = {}
        self._parsed_xml: lxml.etree._ElementTree = None
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
            raise TypeError(f"Missing configuration parameter: {e}")

        # open the manifest xml
        super().open(mode=mode)
        self._xml_fobj = open(self.url, mode="r")

    def close(self) -> None:
        super().close()
        self._xml_fobj = None

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

        if self._parsed_xml is None:
            self._parse_xml()

        # create an EOGroup and set its attributes with a dictionary containing CF and OM_EOP
        from eopf.product.core import EOGroup

        if self._metada_mapping:
            self._attrs = self.translate_attributes(self._metada_mapping)
        else:
            self._attrs = {}

        eog: EOGroup = EOGroup("product_metadata", attrs=self._attrs)
        return eog

    def translate_attributes(self, attributes_dict: Any) -> Any:
        """Used to convert values from metadata mapping

        Parameters
        ----------
        attributes_dict: dict
            dictionary containing metadata

        Returns
        ----------
        internal_dict: dict
            dictionary containing converted values (using apply_xpath or conversion functions)
        """
        # This function is used to parse an convert each value from attributes dictionary
        internal_dict = dict()
        for key, value in attributes_dict.items():
            # Recursive call for nested dictionaries
            if isinstance(value, dict):
                internal_dict[key] = self.translate_attributes(value)
                continue
            # Skip non-string formatted elements (list)
            if isinstance(value, list):
                internal_dict[key] = self.translate_list_attributes(value, key)
                continue
            # Directly convert value if xpath is valid
            if self.is_valid_xpath(value):
                internal_dict[key] = apply_xpath(self._parsed_xml, value, self._namespaces)
                continue
            else:
                # If xpath is invalid, it may contain a conversion function reference <<to_float(xpath)>>
                stac_conversion = self.stac_mapper(value)
                if stac_conversion is not None:
                    internal_dict[key] = stac_conversion
                    continue
                else:
                    # If xpath is invalid, and doesn't containt a conversion function reference
                    raise KeyError(f"{value} is an invalid xpath expression!")
        return internal_dict

    def translate_list_attributes(self, attributes_list: List[Any], global_key: str = None) -> Any:
        """Used to convert values from metadata mapping

        Parameters
        ----------
        attributes_list: list
            list containing metadata

        Returns
        ----------
        local_list_of_dicts: List[dict]
            A list of dictionaries containing converted values in the same nesting as input list.
        """
        local_list_of_dicts = []
        local_dict = dict()
        # Iterate through input list
        for idx in attributes_list:
            if isinstance(idx, str):
                converted_value = self.stac_mapper(idx)
                return [converted_value] if converted_value is not None else [idx]
            if isinstance(idx, list):
                # Recursive call for nested lists
                local_dict[global_key] = self.translate_list_attributes(idx)
                continue
            if isinstance(idx, dict):
                # Iterate over a dictionary ## maybe use local_dict[global_key] = translate_attributes(idx)
                # In order to cover nested dictionaries inside a list
                for key, value in idx.items():
                    if isinstance(value, list):
                        local_dict[key] = self.translate_list_attributes(value)
                        continue

                    stac_conversion = self.stac_mapper(value)
                    local_dict[key] = stac_conversion

            local_list_of_dicts.append(local_dict)
        return local_list_of_dicts

    def _get_xml_data(self, xpath: str) -> Any:
        """Used to get data from xml file

        Parameters
        ----------
        xpath: str
            xpath

        Returns
        ----------
        Any
            xml acquired data
        """
        if not self.is_valid_xpath(xpath):
            return None

        xml_data = self._parsed_xml.xpath(xpath, namespaces=self._namespaces)[0]

        if isinstance(xml_data, _ElementUnicodeResult):
            # Convert ElementUnicodeResult to string
            return str(xml_data)
        elif hasattr(xml_data, "text"):
            if xml_data.text is None:
                # Case where data is stored as attribute eg: <olci:invalidPixels value="749556" percentage="4.000000"/>
                return xml_data.values()[0]
            else:
                # Nominal case, eg: <olci:alTimeSampling>44001</olci:alTimeSampling>
                return xml_data.text
        else:
            return None

    def _get_nc_data(self, path_and_dims: str) -> Any:
        """Used to get data from netCDF file

        Parameters
        ----------
        path_and_dims: str
            a path to a netCDF file followed by requested dims

        Returns
        ----------
        Any
            netCDF acquired data
        """

        # parse the input string for relative file path
        # and wanted dims, perform checks on them
        rel_file_path, wanted_dims = path_and_dims.split(":")
        file_path = (Path(self.url).parent / rel_file_path).resolve()
        if not file_path.is_file():
            raise XmlManifestNetCDFError(f"NetCDF file {file_path} does NOT exist")
        if len(wanted_dims) < 1:
            raise XmlManifestNetCDFError("No dimensions are required")

        # create a dict with the requested dims
        ret_dict: Dict[str, Union[None, int]] = {w: None for w in wanted_dims.split(",")}

        # open netcdf file and read the dims and shape of fist var
        try:
            ncs = EONetCDFStore(str(file_path))
            ncs.open()
            var = ncs[list(ncs.iter("/"))[0]]
            if isinstance(var, EOVariable):
                var_dims = var.dims
                var_shp = var.data.shape
            else:
                # practically not possible
                raise XmlManifestNetCDFError(f"Expected EOVariable not {type(var)}")
        finally:
            ncs.close()

        # iter over the the val dims and populate the requested dims
        for i in range(len(var_dims)):
            if var_dims[i] in ret_dict.keys():
                ret_dict[var_dims[i]] = var_shp[i]

        # check if all requested dims were populated
        for k in ret_dict.keys():
            if ret_dict[k] is None:
                raise XmlManifestNetCDFError(f"Dim {k} not found")

        return ret_dict

    def stac_mapper(self, path: str) -> Any:
        """Used to handle xpath's that request a conversion

        Parameters
        ----------
        path: str
            xpath which may contain formatters

        Returns
        ----------
        Any:
            output of the data getters either xml, netCDF
        """
        image_size_formatter_name = ToImageSize.name
        text_formatter_name = Text.name
        optional_formatter_name = IsOptional.name
        # parse the path
        formatter_name, formatter, xpath = EOFormatterFactory().get_formatter(path)

        if formatter_name is not None and formatter is not None:
            # Handle special formatters parameters (text, netcdf)
            if formatter_name == text_formatter_name:
                return formatter(xpath)
            elif formatter_name == optional_formatter_name:
                data = self._get_xml_data(xpath)
                if data is None:
                    # If xpath cannot be read, check for nested wrapper
                    _, wf, wxpth = EOFormatterFactory().get_formatter(xpath)
                    nested_data = self._get_xml_data(wxpth)
                    if nested_data is not None:
                        # Return nested wrapper if it contains data
                        return wf(nested_data)  # type: ignore
                    # Else, if there is no nested wrapper and data is empty, return is_optional()
                    return formatter(xpath)
                # Return data if found
                return data
            elif formatter_name == image_size_formatter_name:
                return formatter(self._get_nc_data(xpath))
            # if formatter is defined, return it
            return formatter(self._get_xml_data(xpath))
        # If formatter is not defined, just read xpath and return data
        return self._get_xml_data(xpath)

    def is_valid_xpath(self, path: str) -> bool:
        """Used verify if a xpath is valid (output of querry contains any kind of data)

        Parameters
        ----------
        path: str
            xpath

        Returns
        ----------
        Boolean:
            False if xpath is incorrect or doesn't return any data, True otherwise
        """
        try:
            result = apply_xpath(self._parsed_xml, path, namespaces=self._namespaces)
        except KeyError:
            # Return false for lxml parsing errors
            return False
        # Return true if output is not void
        return True if result != "" else False

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

    def __len__(self) -> int:
        """Has no functionality within this store"""
        raise NotImplementedError()

    def __setitem__(self, key: str, value: "EOObject") -> None:
        raise NotImplementedError()

    def is_group(self, path: str) -> bool:
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if path in ["", "/"]:
            return True
        raise NotImplementedError()

    def is_variable(self, path: str) -> bool:
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if path in ["", "/"]:
            return False
        raise NotImplementedError()

    def iter(self, path: str) -> Iterator[str]:
        """Has no functionality within this store"""
        if self._xml_fobj is None:
            raise StoreNotOpenError("Store must be open before access to it")
        yield from ()

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        raise NotImplementedError()
