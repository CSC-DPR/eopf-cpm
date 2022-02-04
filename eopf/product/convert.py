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

from .constants import (
    CF_MAP_OLCI_L1,
    CF_MAP_SLSTR_L1,
    EOP_MAP_OLCI_L1,
    EOP_MAP_SLSTR_L1,
    NAMESPACES_OLCI_L1,
    NAMESPACES_SLSTR_L1,
)
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
    def read(self) -> bool:
        """build product mandatory"""

    def write(self, store_or_path_url: Union[EOProductStore, str, None]) -> None:
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
        cf = {attr: apply_xpath(xfdu_dom, CF_MAP_OLCI_L1[attr], NAMESPACES_OLCI_L1) for attr in CF_MAP_OLCI_L1}
        eop = {
            attr: translate_structure(EOP_MAP_OLCI_L1[attr], xfdu_dom, NAMESPACES_OLCI_L1) for attr in EOP_MAP_OLCI_L1
        }
        attributes_eog = EOGroup("attributes", product=self.eop, attrs={"CF": cf, "OM-EOP": eop, "XFDU": xfdu})
        self.eop.add_group("attributes")
        self.eop["attributes"] = attributes_eog

    def _build_coordinates_eog(self) -> None:
        """build coordinates eog mandatory"""

        coordinates: EOGroup = self.eop.add_group("coordinates")

        # group by ImageGrid
        eog_name = "image_grid"
        image_grid = coordinates.add_group(eog_name)
        file_names = ["time_coordinates", "geo_coordinates"]
        files = filter_files_by(self.nc_files, file_names)
        for f in files:
            if "tie_geo_coordinates" in f:
                files.remove(f)
        xrd = read_xrd(files)
        if xrd:
            for key, value in xrd_to_eovs(xrd):
                image_grid.add_variable(str(key), data=value)

        # group by TiePointGrid
        eog_name = "tie_point_grid"
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
        eog_name = "radiances"
        radiances = measurements.add_group(eog_name)
        file_names = ["Oa%02d_radiance" % r for r in range(1, 22)]
        files = filter_files_by(self.nc_files, file_names)
        to_skip = ["orphan"]
        xrd = read_xrd(files, skip=to_skip)
        if xrd:
            for key, value in xrd_to_eovs(xrd):
                radiances.add_variable(str(key), data=value)

        eog_name = "orphans"
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
        eog_name = "geometry"
        geometry = conditions.add_group(eog_name)
        file_names = ["tie_geometries"]
        files = filter_files_by(self.nc_files, file_names)
        xrd = read_xrd(files)
        if xrd:
            for key, value in xrd_to_eovs(xrd):
                geometry.add_variable(str(key), data=value)

        # group by Meteo
        eog_name = "meteo"
        meteo = conditions.add_group(eog_name)
        file_names = ["tie_meteo"]
        files = filter_files_by(self.nc_files, file_names)
        xrd = read_xrd(files)
        if xrd:
            for key, value in xrd_to_eovs(xrd):
                meteo.add_variable(str(key), data=value)

        # group by InstrumentData
        eog_name = "instrument_data"
        instrument_data = conditions.add_group(eog_name)
        file_names = ["instrument_data"]
        files = filter_files_by(self.nc_files, file_names)
        xrd = read_xrd(files)
        if xrd:
            for key, value in xrd_to_eovs(xrd):
                instrument_data.add_variable(str(key), data=value)

    def read(self) -> bool:
        """build product mandatory"""

        if self._check_input():
            self._build_attributes_eog()
            self._build_coordinates_eog()
            self._build_measurements_eog()
            self._build_quality_eog()
            self._build_conditions_eog()

        return self.eop.is_valid()


class SLSTRL1EOPConverter(S3L1EOPConverter):
    """Converter from the legacy safe S3 SLSTR L1 product to EOProduct format

    Attributes
    ----------
    input_path
    """

    def __init__(self, input_prod_path: str):
        super().__init__(input_prod_path)
        self.RASTER_EXT_FIRE = ["fn", "fo"]
        self.RASTER_EXT_INFRA = ["in", "io"]
        self.RASTER_EXT_1KM = self.RASTER_EXT_FIRE + self.RASTER_EXT_INFRA
        self.RASTER_EXT_3O0M = ["an", "ao", "bn", "bo"]
        self.RASTER_EXT = self.RASTER_EXT_1KM + self.RASTER_EXT_3O0M
        self.nc_files = get_dir_files(self.input_path, "*.nc")
        self.TIME_EXT = ["a", "b", "i"]
        self.BT_CHANNELS = ["F1", "F2", "S7", "S8", "S9"]
        self.RAD_CHANNELS = ["S1", "S2", "S3", "S4", "S5", "S6"]

    def _build_attributes_eog(self) -> None:
        """build attributes eog mandatory"""
        xfdu_dom = parse_xml(self.input_path, "xfdumanifest.xml")
        xml_path = os.path.join(self.input_path, "xfdumanifest.xml")
        tree = ET.parse(xml_path)
        root = tree.getroot()
        xfdu = etree_to_dict(root[1])
        cf = {attr: apply_xpath(xfdu_dom, CF_MAP_SLSTR_L1[attr], NAMESPACES_SLSTR_L1) for attr in CF_MAP_SLSTR_L1}
        eop = {
            attr: translate_structure(EOP_MAP_SLSTR_L1[attr], xfdu_dom, NAMESPACES_SLSTR_L1)
            for attr in EOP_MAP_SLSTR_L1
        }
        attributes_eog = EOGroup("Attributes", product=self.eop, attrs={"CF": cf, "OM-EOP": eop, "XFDU": xfdu})
        self.eop["Attributes"] = attributes_eog

    def _build_coordinates_eog(self) -> None:
        """build coordinates eog mandatory"""
        coordinates_eog: EOGroup = self.eop.add_group("coordinates")

        # group by self.RASTER_EXT
        for g in self.RASTER_EXT:
            file_names = [file_pattern.format(g=g) for file_pattern in ["cartesian_{g}", "geodetic_{g}"]]
            files = filter_files_by(self.nc_files, file_names)
            to_skip = [
                var_pattern.format(g=g)
                for var_pattern in [
                    "elevation_{g}",
                    "elevation_orphan_{g}",
                    "x_orphan_{g}",
                    "y_orphan_{g}",
                    "latitude_orphan_{g}",
                    "longitude_orphan_{g}",
                ]
            ]
            xrd = read_xrd(files, skip=to_skip)
            if xrd:
                eog_name = "image_grid_{g}".format(g=g)
                image_grid_eog: EOGroup = coordinates_eog.add_group(eog_name)
                for key, value in xrd_to_eovs(xrd):
                    image_grid_eog.add_variable(str(key), data=value)

        # orphan data goes into one group
        image_grid_orphan_eog: EOGroup = coordinates_eog.add_group("image_grid_orphan")
        for g in self.RASTER_EXT:
            file_names = [file_pattern.format(g=g) for file_pattern in ["cartesian_{g}", "geodetic_{g}"]]
            files = filter_files_by(self.nc_files, file_names)
            to_skip = [
                var_pattern.format(g=g)
                for var_pattern in [
                    "elevation_{g}",
                    "elevation_orphan_{g}",
                    "x_{g}",
                    "y_{g}",
                    "latitude_{g}",
                    "longitude_{g}",
                ]
            ]
            xrd = read_xrd(files, skip=to_skip)
            if xrd:
                g_eog = image_grid_orphan_eog.add_group(g)
                for key, value in xrd_to_eovs(xrd):
                    g_eog.add_variable(str(key), data=value)

        # tiepoints
        tie_point_grid_eog = coordinates_eog.add_group("tie_point_grid")
        file_names = ["cartesian_tx", "geodetic_tx"]
        files = filter_files_by(self.nc_files, file_names)
        xrd = read_xrd(files)
        if xrd:
            for key, value in xrd_to_eovs(xrd):
                tie_point_grid_eog.add_variable(str(key), data=value)

        # group time by TIME_EXT
        for g in self.TIME_EXT:
            eog_name = "Time_{g}".format(g=g)
            time_g_eog: EOGroup = coordinates_eog.add_group(eog_name)
            file_names = ["time_{g}n".format(g=g)]
            files = filter_files_by(self.nc_files, file_names)
            to_pick = ["time_stamp_{g}".format(g=g)]
            xrd = read_xrd(files, pick=to_pick)
            if xrd:
                for key, value in xrd_to_eovs(xrd):
                    time_g_eog.add_variable(str(key), data=value)

    def _build_measurements_eog(self) -> None:
        """build coordinates eog mandatory"""
        measurements_eog: EOGroup = self.eop.add_group("measurements")

        # group brightness_temperatures by RASTER_EXT_1KM
        for g in self.RASTER_EXT_1KM:
            eog_name = "brightness_temperatures_{g}".format(g=g)
            btg_eog = measurements_eog.add_group(eog_name)
            for ch in self.BT_CHANNELS:
                file_names = ["{ch}_BT_{g}".format(ch=ch, g=g)]
                files = filter_files_by(self.nc_files, file_names)
                to_skip = [
                    var_pattern.format(ch=ch, g=g)
                    for var_pattern in ["{ch}_exception_{g}", "{ch}_BT_orphan_{g}", "{ch}_exception_orphan_{g}"]
                ]
                xrd = read_xrd(files, skip=to_skip)
                if xrd:
                    for key, value in xrd_to_eovs(xrd):
                        btg_eog.add_variable(str(key), data=value)

        # group radiances by RASTER_EXT_3O0M
        for g in self.RASTER_EXT_3O0M:
            eog_name = "radiances_{g}".format(g=g)
            rad_g_eog: EOGroup = measurements_eog.add_group(eog_name)
            for ch in self.RAD_CHANNELS:
                file_names = ["{ch}_radiance_{g}".format(ch=ch, g=g)]
                files = filter_files_by(self.nc_files, file_names)
                to_skip = [
                    var_pattern.format(ch=ch, g=g)
                    for var_pattern in ["{ch}_exception_{g}", "{ch}_radiance_orphan_{g}", "{ch}_exception_orphan_{g}"]
                ]
                xrd_rad = read_xrd(files, skip=to_skip)
                if xrd_rad:
                    for key, value in xrd_to_eovs(xrd_rad):
                        rad_g_eog.add_variable(str(key), data=value)

        # group BT orphans
        eog_name = "brightness_temperatures_orphan"
        bto_eog: EOGroup = measurements_eog.add_group(eog_name)
        for g in self.RASTER_EXT_1KM:
            eog_raster_name = g
            bto_g_eog: EOGroup = bto_eog.add_group(eog_raster_name)
            for ch in self.BT_CHANNELS:
                file_names = ["{ch}_BT_{g}".format(ch=ch, g=g)]
                files = filter_files_by(self.nc_files, file_names)
                to_skip = [
                    var_pattern.format(ch=ch, g=g)
                    for var_pattern in ["{ch}_exception_{g}", "{ch}_BT_{g}", "{ch}_exception_orphan_{g}"]
                ]
                xrd = read_xrd(files, skip=to_skip)
                if xrd:
                    for key, value in xrd_to_eovs(xrd):
                        bto_g_eog.add_variable(str(key), data=value)

        # group radiance orphans by RASTER_EXT_3O0M
        eog_name = "radiances_orphan"
        rado_eog: EOGroup = measurements_eog.add_group(eog_name)
        for g in self.RASTER_EXT_3O0M:
            eog_raster_name = g
            g_eog: EOGroup = rado_eog.add_group(eog_raster_name)
            for ch in self.RAD_CHANNELS:
                file_names = ["{ch}_radiance_{g}".format(ch=ch, g=g)]
                files = filter_files_by(self.nc_files, file_names)
                to_skip = [
                    var_pattern.format(ch=ch, g=g)
                    for var_pattern in ["{ch}_exception_{g}", "{ch}_radiance_{g}", "{ch}_exception_orphan_{g}"]
                ]
                xrd = read_xrd(files, skip=to_skip)
                if xrd:
                    for key, value in xrd_to_eovs(xrd):
                        g_eog.add_variable(str(key), data=value)

    def _build_quality_eog(self) -> None:
        """build quality eog NOT mandatory"""
        quality_eog: EOGroup = self.eop.add_group("quality")

        # group flags by RASTER_EXT_1KM
        for g in self.RASTER_EXT_1KM:
            eog_name = "flags_{g}".format(g=g)
            flags1km_g_eog: EOGroup = quality_eog.add_group(eog_name)

            # read from ch_BT_g files
            for ch in self.BT_CHANNELS:
                file_names = ["{ch}_BT_{g}".format(ch=ch, g=g)]
                files = filter_files_by(self.nc_files, file_names)
                to_pick = ["{ch}_exception_{g}".format(ch=ch, g=g)]
                xrd = read_xrd(files, pick=to_pick)
                if xrd:
                    for key, value in xrd_to_eovs(xrd):
                        flags1km_g_eog.add_variable(str(key), data=value)

            # read from flags_g files
            file_names = ["flags_{g}".format(g=g)]
            files = filter_files_by(self.nc_files, file_names)
            to_skip = [
                var_pattern.format(g=g)
                for var_pattern in [
                    "bayes_orphan_{g}",
                    "cloud_orphan_{g}",
                    "confidence_orphan_{g}",
                    "pointing_orphan_{g}",
                ]
            ]
            xrd = read_xrd(files, skip=to_skip)
            if xrd:
                for key, value in xrd_to_eovs(xrd):
                    flags1km_g_eog.add_variable(str(key), data=value)

        # group flags by RASTER_EXT_3O0M
        for g in self.RASTER_EXT_3O0M:
            eog_name = "flags_{g}".format(g=g)
            flags_g_eog: EOGroup = quality_eog.add_group(eog_name)

            # read from ch_radiance_g files
            for ch in self.RAD_CHANNELS:
                file_names = ["{ch}_radiance_{g}".format(ch=ch, g=g)]
                files = filter_files_by(self.nc_files, file_names)
                to_pick = ["{ch}_exception_{g}".format(ch=ch, g=g)]
                xrd = read_xrd(files, pick=to_pick)
                if xrd:
                    for key, value in xrd_to_eovs(xrd):
                        flags_g_eog.add_variable(str(key), data=value)

            # read from flags_g files
            file_names = ["flags_{g}".format(g=g)]
            files = filter_files_by(self.nc_files, file_names)
            to_skip = [
                var_pattern.format(g=g)
                for var_pattern in [
                    "bayes_orphan_{g}",
                    "cloud_orphan_{g}",
                    "confidence_orphan_{g}",
                    "pointing_orphan_{g}",
                ]
            ]
            xrd = read_xrd(files, skip=to_skip)
            if xrd:
                for key, value in xrd_to_eovs(xrd):
                    flags_g_eog.add_variable(str(key), data=value)

        # orphan data goes to a single group
        flags_orphan_eog: EOGroup = quality_eog.add_group("flags_orphan")

        # group orphans by RASTER_EXT_1KM
        for g in self.RASTER_EXT_1KM:
            eog_name = g
            g1km_eog: EOGroup = flags_orphan_eog.add_group(eog_name)

            # read from ch_BT_g files
            for ch in self.BT_CHANNELS:
                file_names = ["{ch}_BT_{g}".format(ch=ch, g=g)]
                files = filter_files_by(self.nc_files, file_names)
                to_pick = ["{ch}_exception_orphan_{g}".format(ch=ch, g=g)]
                xrd = read_xrd(files, pick=to_pick)
                if xrd:
                    for key, value in xrd_to_eovs(xrd):
                        g1km_eog.add_variable(str(key), data=value)

            # read from flags_g files
            file_names = ["flags_{g}".format(g=g)]
            files = filter_files_by(self.nc_files, file_names)
            to_skip = [
                var_pattern.format(g=g)
                for var_pattern in [
                    "bayes_{g}",
                    "cloud_{g}",
                    "confidence_{g}",
                    "pointing_{g}",
                    "probability_cloud_dual_{g}",
                    "probability_cloud_single_{g}",
                ]
            ]
            xrd = read_xrd(files, skip=to_skip)
            if xrd:
                for key, value in xrd_to_eovs(xrd):
                    g1km_eog.add_variable(str(key), data=value)

        # group orphans by RASTER_EXT_3O0M
        for g in self.RASTER_EXT_3O0M:
            eog_name = g
            g_eog: EOGroup = flags_orphan_eog.add_group(eog_name)

            # read from ch_radiance_g files
            rad_bt_channels = self.BT_CHANNELS + self.RAD_CHANNELS
            for ch in rad_bt_channels:
                file_names = ["{ch}_radiance_{g}".format(ch=ch, g=g)]
                files = filter_files_by(self.nc_files, file_names)
                to_pick = ["{ch}_exception_orphan_{g}".format(ch=ch, g=g)]
                xrd = read_xrd(files, pick=to_pick)
                if xrd:
                    for key, value in xrd_to_eovs(xrd):
                        g_eog.add_variable(str(key), data=value)

            # read from flags_g files
            file_names = ["flags_{g}".format(g=g)]
            files = filter_files_by(self.nc_files, file_names)
            to_skip = [
                var_pattern.format(g=g)
                for var_pattern in [
                    "bayes_{g}",
                    "cloud_{g}",
                    "confidence_{g}",
                    "pointing_{g}",
                    "probability_cloud_dual_{g}",
                    "probability_cloud_single_{g}",
                ]
            ]
            xrd = read_xrd(files, skip=to_skip)
            if xrd:
                for key, value in xrd_to_eovs(xrd):
                    g_eog.add_variable(str(key), data=value)

    def _build_conditions_eog(self) -> None:
        """build conditions eog NOT mandatory"""
        conditions_eog: EOGroup = self.eop.add_group("conditions")

        # group Geodetic by RASTER_EXT
        for g in self.RASTER_EXT:
            eog_name = "geodetic_{g}".format(g=g)
            geodetic_g_eog: EOGroup = conditions_eog.add_group(eog_name)
            file_names = ["geodetic_{g}".format(g=g)]
            files = filter_files_by(self.nc_files, file_names)
            to_skip = [
                var_pattern.format(g=g)
                for var_pattern in [
                    "latitude_{g}",
                    "latitude_orphan_{g}",
                    "longitude_{g}",
                    "longitude_orphan_{g}",
                    "elevation_orphan_{g}",
                ]
            ]
            xrd = read_xrd(files, skip=to_skip)
            if xrd:
                for key, value in xrd_to_eovs(xrd):
                    geodetic_g_eog.add_variable(str(key), data=value)

        # group Geodetic by RASTER_EXT
        eog_name = "geodetic_orphan"
        geodetico_eog: EOGroup = conditions_eog.add_group(eog_name)
        for g in self.RASTER_EXT:
            eog_raster_name = g
            g_eog: EOGroup = geodetico_eog.add_group(eog_raster_name)
            file_names = ["geodetic_{g}".format(g=g)]
            files = filter_files_by(self.nc_files, file_names)
            to_pick = ["elevation_orphan_{g}".format(g=g)]
            xrd = read_xrd(files, pick=to_pick)
            if xrd:
                for key, value in xrd_to_eovs(xrd):
                    g_eog.add_variable(str(key), data=value)

        # group Geometry by tn to
        for g in ["tn", "to"]:
            eog_name = "Geometry_{g}".format(g=g)
            geometry_g_eog: EOGroup = conditions_eog.add_group(eog_name)
            file_names = ["geometry_{g}".format(g=g)]
            files = filter_files_by(self.nc_files, file_names)
            xrd = read_xrd(files)
            if xrd:
                for key, value in xrd_to_eovs(xrd):
                    geometry_g_eog.add_variable(str(key), data=value)

        # InstrumentData grouping
        # group InstrumentData by RASTER_EXT_1KM
        for g in self.RASTER_EXT_1KM:
            eog_raster_name = "instrument_data_{g}".format(g=g)
            instrumentdata1km_g_eog: EOGroup = conditions_eog.add_group(eog_raster_name)
            for ch in self.BT_CHANNELS:
                file_names = ["{ch}_quality_{g}".format(ch=ch, g=g)]
                files = filter_files_by(self.nc_files, file_names)
                xrd = read_xrd(files)
                if xrd:
                    eog_name = ch
                    ch1km_g: EOGroup = instrumentdata1km_g_eog.add_group(eog_name)
                    for key, value in xrd_to_eovs(xrd):
                        ch1km_g.add_variable(str(key), data=value)

        # group InstrumentData by [an, ao]
        for g in ["an", "ao"]:
            eog_raster_name = "instrument_data_{g}".format(g=g)
            instrumentdata_anao_g_eog: EOGroup = conditions_eog.add_group(eog_raster_name)
            for ch in self.RAD_CHANNELS:
                eog_name = ch
                chrad_g: EOGroup = instrumentdata_anao_g_eog.add_group(eog_name)
                file_names = ["{ch}_quality_{g}".format(ch=ch, g=g)]
                files = filter_files_by(self.nc_files, file_names)
                xrd = read_xrd(files, file_names)
                if xrd:
                    for key, value in xrd_to_eovs(xrd):
                        chrad_g.add_variable(str(key), data=value)

        # group InstrumentData by [bn, bo]
        for g in ["bn", "bo"]:
            eog_raster_name = "instrument_data_{g}".format(g=g)
            instrumentdata_g_eog: EOGroup = conditions_eog.add_group(eog_raster_name)
            for ch in ["S4", "S5", "S6"]:
                eog_name = ch
                chs_g: EOGroup = instrumentdata_g_eog.add_group(eog_name)
                file_names = ["{ch}_quality_{g}".format(ch=ch, g=g)]
                files = filter_files_by(self.nc_files, file_names)
                xrd = read_xrd(files, file_names)
                if xrd:
                    for key, value in xrd_to_eovs(xrd):
                        chs_g.add_variable(str(key), data=value)

        # group ProcessingData by self.RASTER_EXT
        for g in self.RASTER_EXT:
            eog_name = "processing_data_{g}".format(g=g)
            pdg_eog: EOGroup = conditions_eog.add_group(eog_name)
            file_names = ["indices_{g}".format(g=g)]
            files = filter_files_by(self.nc_files, file_names)
            to_skip = [
                var_pattern.format(g=g)
                for var_pattern in ["detector_orphan_{g}", "pixel_orphan_{g}", "scan_orphan_{g}"]
            ]
            xrd = read_xrd(files, skip=to_skip)
            if xrd:
                for key, value in xrd_to_eovs(xrd):
                    pdg_eog.add_variable(str(key), data=value)

        # orphan data goes to a single group
        eog_orphan_name = "processing_data_orphan"
        pdo_eog: EOGroup = conditions_eog.add_group(eog_orphan_name)

        # group ProcessingDataOrphan by RASTER_EXT
        for g in self.RASTER_EXT:
            eog_name = g
            ch_g: EOGroup = pdo_eog.add_group(eog_name)
            file_names = ["indices_{g}".format(g=g)]
            files = filter_files_by(self.nc_files, file_names)
            to_skip = [
                var_pattern.format(g=g)
                for var_pattern in ["detector_{g}", "pixel_{g}", "scan_{g}", "l0_scan_offset_{g}"]
            ]
            xrd = read_xrd(files, skip=to_skip)
            if xrd:
                for key, value in xrd_to_eovs(xrd):
                    ch_g.add_variable(str(key), data=value)

        # group Time by TIME_EXT
        for g in self.TIME_EXT:
            eog_name = "time_{g}".format(g=g)
            time_g_eog: EOGroup = conditions_eog.add_group(eog_name)
            file_names = ["time_{g}n".format(g=g)]
            files = filter_files_by(self.nc_files, file_names)
            to_skip = ["time_stamp_{g}".format(g=g)]
            xrd = read_xrd(files, skip=to_skip)
            if xrd:
                for key, value in xrd_to_eovs(xrd):
                    time_g_eog.add_variable(str(key), data=value)

        # viscal group
        eog_name = "viscal"
        viscal_eog: EOGroup = conditions_eog.add_group(eog_name)
        file_names = ["viscal"]
        files = filter_files_by(self.nc_files, file_names)
        xrd = read_xrd(files)
        if xrd:
            for key, value in xrd_to_eovs(xrd):
                viscal_eog.add_variable(str(key), data=value)

        # meteo group
        eog_name = "meteo"
        meteo_eog: EOGroup = conditions_eog.add_group(eog_name)
        file_names = ["met_tx"]
        files = filter_files_by(self.nc_files, file_names)
        xrd = read_xrd(files)
        if xrd:
            for key, value in xrd_to_eovs(xrd):
                meteo_eog.add_variable(str(key), data=value)

    def read(self) -> bool:
        """build product mandatory"""

        if self._check_input():
            self._build_attributes_eog()
            self._build_coordinates_eog()
            self._build_measurements_eog()
            self._build_quality_eog()
            self._build_conditions_eog()

        return self.eop.is_valid()
