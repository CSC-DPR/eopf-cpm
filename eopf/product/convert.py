import abc
import os
from typing import Union

import defusedxml.ElementTree as ET

from eopf.exceptions import StoreNotDefinedError
from eopf.product.conveniences import (
    apply_xpath,
    etree_to_dict,
    filter_files_by,
    get_dir_files,
    parse_xml,
    read_xrd,
    translate_structure,
    xrd_to_eovs,
)
from eopf.product.core import EOGroup, EOProduct

from .constants import CF_MAP_OLCI, EOP_MAP_OLCI, NAMESPACES_OLCI
from .store.abstract import EOProductStore

""""
Provide some converter from L0, L1 or L2 baseline products
of S1, S2 and S3 land to EOProduct.
"""


class S3L1EOPConverter:
    """Base converter from the Legacy safe S3 L1 products to EO format

    Attributes
    ----------
    input_path
    """

    def __init__(self, input_path: str):
        self.input_path = input_path
        self.input_prod_name = os.path.basename(self.input_path)
        self.eop = EOProduct(self.input_prod_name)

    def _check_input(self) -> bool:
        """check if the input path is a directory

        Returns:
        True if input_path is directory, False otherwise : bool
        """
        if not os.path.isdir(self.input_path):
            return False

        return True

    @abc.abstractmethod
    def build_coordinates_eog(self) -> None:
        """build coordinates eog mandatory"""

    @abc.abstractmethod
    def build_attributes_eog(self) -> None:
        """build coordinates eog mandatory"""

    @abc.abstractmethod
    def build_measurements_eog(self) -> None:
        """build coordinates eog mandatory"""

    @abc.abstractmethod
    def build_eop(self) -> bool:
        """build product mandatory"""

    def write_eop(self, store_or_path_url: Union[EOProductStore, str, None]) -> None:
        """write product mandatory"""

        try:
            with self.eop.open(mode="w", store_or_path_url=store_or_path_url):
                self.eop.write()
        except StoreNotDefinedError:
            pass
        except Exception as e:
            print(f"Failed to open the path or store. Error {e}")


class OLCIL1EOPConverter(S3L1EOPConverter):
    """Converter from the legacy safe S3 SLSTR L1 product to EOProduct format

    Attributes
    ----------
    input_path
    """

    def __init__(self, input_prod_path: str):
        super().__init__(input_prod_path)
        self.nc_files = get_dir_files(self.input_path, "*.nc")

    def _build_attributes_eog(self) -> None:
        """build attributes eog mandatory"""

        xfdu_dom = parse_xml(self.input_path, "xfdumanifest.xml")
        xml_path = os.path.join(self.input_path, "xfdumanifest.xml")
        tree = ET.parse(xml_path)
        root = tree.getroot()
        xfdu = etree_to_dict(root[1])
        cf = {attr: apply_xpath(xfdu_dom, CF_MAP_OLCI[attr], NAMESPACES_OLCI) for attr in CF_MAP_OLCI}
        eop = {attr: translate_structure(EOP_MAP_OLCI[attr], xfdu_dom, NAMESPACES_OLCI) for attr in EOP_MAP_OLCI}
        attributes_eog = EOGroup("attributes", product=self.eop, attrs={"CF": cf, "OM-EOP": eop, "XFDU": xfdu})
        self.eop.add_group("attributes")
        self.eop["attributes"] = attributes_eog

    def _build_coordinates_eog(self) -> None:
        """build coordinates eog mandatory"""

        coordinates: EOGroup = self.eop.add_group("coordinates")

        # group by ImageGrid
        eog_name = "ImageGrid"
        image_grid = coordinates.add_group(eog_name)
        file_names = ["time_coordinates", "geo_coordinates"]
        files = filter_files_by(self.nc_files, file_names)
        xrd = read_xrd(files)
        if xrd:
            for key, value in xrd_to_eovs(xrd):
                image_grid.add_variable(str(key), data=value)

        # group by TiePointGrid
        eog_name = "TiePointGrid"
        tie_point_grid = coordinates.add_group(eog_name)
        file_names = ["tie_geo_coordinates"]
        files = filter_files_by(self.nc_files, file_names)
        xrd = read_xrd(files)
        if xrd:
            for key, value in xrd_to_eovs(xrd):
                tie_point_grid.add_variable(str(key), data=value)

    def _build_measurements_eog(self) -> None:
        """build measurements eog mandatory"""
        measurements = self.eop.add_group("measurements")

        # group by Radiances
        eog_name = "Radiances"
        radiances = measurements.add_group(eog_name)
        file_names = ["Oa%02d_radiance" % r for r in range(1, 22)]
        files = filter_files_by(self.nc_files, file_names)
        to_skip = ["orphan"]
        xrd = read_xrd(files, skip=to_skip)
        if xrd:
            for key, value in xrd_to_eovs(xrd):
                radiances.add_variable(str(key), data=value)

        eog_name = "Orphans"
        orphans = measurements.add_group(eog_name)
        file_names = ["removed_pixels"]
        files = filter_files_by(self.nc_files, file_names)
        xrd = read_xrd(files)
        if xrd:
            for key, value in xrd_to_eovs(xrd):
                orphans.add_variable(str(key), data=value)

    def _build_quality_eog(self) -> None:
        """build quality eog mandatory"""
        quality = self.eop.add_group("quality")

        file_names = ["qualityFlags"]
        files = filter_files_by(self.nc_files, file_names)
        xrd = read_xrd(files)
        if xrd:
            for key, value in xrd_to_eovs(xrd):
                quality.add_variable(str(key), data=value)

    def _build_conditions_eog(self) -> None:
        """build conditions eog mandatory"""
        conditions = self.eop.add_group("conditions")

        # group by Geometry
        eog_name = "Geometry"
        geometry = conditions.add_group(eog_name)
        file_names = ["tie_geometries"]
        files = filter_files_by(self.nc_files, file_names)
        xrd = read_xrd(files)
        if xrd:
            for key, value in xrd_to_eovs(xrd):
                geometry.add_variable(str(key), data=value)

        # group by Meteo
        eog_name = "Meteo"
        meteo = conditions.add_group(eog_name)
        file_names = ["tie_meteo"]
        files = filter_files_by(self.nc_files, file_names)
        xrd = read_xrd(files)
        if xrd:
            for key, value in xrd_to_eovs(xrd):
                meteo.add_variable(str(key), data=value)

        # group by InstrumentData
        eog_name = "InstrumentData"
        instrument_data = conditions.add_group(eog_name)
        file_names = ["instrument_data"]
        files = filter_files_by(self.nc_files, file_names)
        xrd = read_xrd(files)
        if xrd:
            for key, value in xrd_to_eovs(xrd):
                instrument_data.add_variable(str(key), data=value)

    def build_eop(self) -> bool:
        """build product mandatory"""

        if self._check_input():
            self._build_attributes_eog()
            self._build_coordinates_eog()
            self._build_measurements_eog()
            self._build_quality_eog()
            self._build_conditions_eog()

        return self.eop.is_valid()
