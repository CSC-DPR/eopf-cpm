import json
import os
from pathlib import Path

import numpy as np
import pytest
from pytest_lazyfixture import lazy_fixture

from eopf.product.conveniences import open_store
from eopf.product.core import EOProduct, EOVariable
from eopf.product.core.eo_object import EOObject
from eopf.product.store import (  # , EOCogStore
    EONetCDFStore,
    EOProductStore,
    EOZarrStore,
)
from eopf.product.store.conveniences import convert
from eopf.product.store.mapping_factory import EOMappingFactory
from eopf.product.store.safe import EOSafeStore
from eopf.product.utils import conv
from tests.utils import assert_eovariable_equal


##########################################################################################
# run specific test using keyword search feature of pytest, with logical expresions, ex:
# python3.9 -m pytest test_safe_store_mappings.py::test_convert_safe_mapping -k "S2 and zarr"
##########################################################################################
@pytest.mark.need_files
@pytest.mark.unit
@pytest.mark.parametrize(
    "input_path, key_path",
    [
        # fmt: off
        # At least One test per each product type and accessor used in the mapping file
        # "item_format": "filename_to_subswath",
        # (lazy_fixture("S1_IM_OCN_ZIP"), "/coordinates/osw/sub_swath"),
        # "item_format": "netcdf"
        # (lazy_fixture("S1_IM_OCN_ZIP"), "/coordinates/owi/owiLon"),
        # "item_format": "attribute_element_to_flag_variable",
        # (lazy_fixture("S1_IM_OCN_ZIP"), "/coordinates/owi/owiPolarisationName"),
        # "item_format": "attribute_element_to_float_variable",
        # lazy_fixture("S1_IM_OCN_ZIP"), "/conditions/state_vector/sv_x"),
        # TODO: do we test "item_format": "xmlmetadata", misc ?
        # "item_format": "xmlangles",
        # (lazy_fixture("S2_MSIL1C_ZIP"), "/conditions/geometry/saa"),
        # # "item_format": "jp2",
        # (lazy_fixture("S2_MSIL1C_ZIP"), "/quality/msk_detfoo_b11"),
        # # "item_format": "xmltp",
        # (lazy_fixture("S2_MSIL1C_ZIP"), "/coordinates/tiepoint_grid/y_tp"),
        # # "item_format": "grib",
        # (lazy_fixture("S2_MSIL1C_ZIP"), "/coordinates/meteo/latitude_meteo"),
        # # "item_format": "xmlangles",
        # (lazy_fixture("S2_MSIL1C_ZIP"), "/conditions/geometry/saa"),
        # # "item_format": "xmlangles",
        # (lazy_fixture("S2_MSIL1C_ZIP"), "/conditions/geometry/saa"),
        # "item_format": "netcdf"
        # (lazy_fixture("S3_SY_2_SYN_ZIP"), "/coordinates/image_grid/longitude"),
        # (lazy_fixture("S3_SY_2_SYN_ZIP"), "/conditions/geometry/saa"),
        #(lazy_fixture("S3_SY_2_SYN_ZIP"), "/measurements/olci/sdr_oa01"),
        # "item_format": "netcdf"
        # (lazy_fixture("S3_OL_1_EFR_ZIP"), "/coordinates/image_grid/longitude"),
        # (lazy_fixture("S3_OL_1_EFR_ZIP"), "/conditions/geometry/oza"),
        # (lazy_fixture("S3_OL_1_EFR_ZIP"), "/measurements/orphans/oa21_radiance"),
        # "item_format": "netcdf"
        # (lazy_fixture("S3_OL_2_LFR_ZIP"), "/coordinates/image_grid/latitude"),
        # (lazy_fixture("S3_OL_2_LFR_ZIP"), "/conditions/geometry/oaa"),
        # (lazy_fixture("S3_OL_2_LFR_ZIP"), "/measurements/image/rc681"),
        # "item_format": "netcdf"
        # (lazy_fixture("S3_SL_1_RBT_ZIP"), "/coordinates/an/x_an"),
        # (lazy_fixture("S3_SL_1_RBT_ZIP"), "/conditions/geometry_tn/sat_azimuth_tn"),
        # (lazy_fixture("S3_SL_1_RBT_ZIP"), "/measurements/an/s1_radiance_an"),
        # "item_format": "netcdf"
        # (lazy_fixture("S3_SL_2_LST_ZIP"), "/coordinates/in/latitude_in"),
        # (lazy_fixture("S3_SL_2_LST_ZIP"), "/conditions/orphan/elevation_orphan_in"),
        # (lazy_fixture("S3_SL_2_LST_ZIP"), "/measurements/in/LST"),
        # fmt: on
    ],
)
def test_read_product(dask_client_all, input_path, key_path):
    store = EOSafeStore(input_path)
    product = EOProduct("my_product", storage=store)
    product.open()
    eoval = product[key_path]
    assert isinstance(eoval, EOObject)
    assert isinstance(product.attrs, dict)
    product.store.close()


@pytest.mark.need_files
@pytest.mark.integration
@pytest.mark.parametrize(
    "input_path",
    [
        # lazy_fixture("S3_OL_1_EFR_ZIP"),
        # lazy_fixture("S3_OL_2_LFR_ZIP"),
        # lazy_fixture("S3_SL_1_RBT_ZIP"),
        # lazy_fixture("S3_SL_2_LST_ZIP"),
        # lazy_fixture("S3_SY_2_SYN_ZIP"),
        # not working 04.07
        # lazy_fixture("S2_MSIL1C_ZIP"),
        # lazy_fixture("S2_MSIL1C"),
        # lazy_fixture("S1_IM_OCN_ZIP"),
    ],
)
def test_load_product(dask_client_all, input_path):
    store = EOSafeStore(input_path)
    product = EOProduct("my_product", storage=store)
    product.open()
    assert isinstance(product.attrs, dict)
    product.load()
    assert len(product._groups) > 0
    assert isinstance(product.attrs, dict)
    product.store.close()


@pytest.mark.need_files
@pytest.mark.unit
@pytest.mark.parametrize(
    "input_path, mapping_filename, output_extension, output_store, max_optional_miss",
    [
        # fmt: off
        # (lazy_fixture("TEST_PRODUCT"), "data/test_safe_mapping.json", ".zarr", EOZarrStore, 1),
        # (lazy_fixture("TEST_PRODUCT_ZIP"), "data/test_safe_mapping.json", ".zarr", EOZarrStore, 1),
        # fmt: on
    ],
)
def test_convert_test_mapping(
    dask_client_all,
    input_path: str,
    mapping_filename: str,
    output_extension: str,
    output_store: type[EOProductStore],
    max_optional_miss: int,
    OUTPUT_DIR: str,
):
    mapping_factory = EOMappingFactory(False)
    mapping_filename = str(Path(__file__).parent / mapping_filename)
    mapping_factory.register_mapping(mapping_filename)
    impl_test_convert_safe_mapping(
        input_path,
        mapping_filename,
        output_extension,
        output_store,
        max_optional_miss,
        OUTPUT_DIR,
        mapping_factory,
    )


@pytest.mark.need_files
@pytest.mark.integration
@pytest.mark.parametrize(
    "input_path, mapping_filename, output_extension, output_store, max_optional_miss",
    [
        # fmt: off

        # Target: convert safe to zarr for all level , netcdf (for L1/L2 ), cog(for L1/L2 )
        # S1 (L0 / L1 / OCN L2), S2 L1C / L2, S3 (L0, OLCI L1/L2, SLSTR L1/L2, SRAL L1/L2 , SYN L2 )

        # ####################### S1 product type conversions ########################
        # ---> S1/Level 0 - tests -> MAPPING not available
        # (lazy_fixture("S1_IW_RAW_ZIP"), lazy_fixture("S1_IW_RAW_MAPPING"), ".SAFE", EOSafeStore, 0,),
        # (lazy_fixture("S1_IW_RAW_ZIP"), lazy_fixture("S1_IW_RAW_MAPPING"), ".zarr", EOZarrStore, 0,),

        # ---> S1/Level 1 - tests -> MAPPING not available
        # (lazy_fixture("S1_IW_SLC_ZIP"), lazy_fixture("S1_IW_SLC_MAPPING"), ".SAFE", EOSafeStore, 0,),
        # (lazy_fixture("S1_IW_SLC_ZIP"), lazy_fixture("S1_IW_SLC_MAPPING"), ".zarr", EOZarrStore, 0,),
        # (lazy_fixture("S1_IW_SLC_ZIP"), lazy_fixture("S1_IW_SLC_MAPPING"), ".cog", EOCogStore, 0,),
        # (lazy_fixture("S1_IW_SLC_ZIP"), lazy_fixture("S1_IW_SLC_MAPPING"), ".nc", EONetCDFStore, 0,),
        # (lazy_fixture("S1_IW_GRD_ZIP"), lazy_fixture("S1_IW_GRD_MAPPING"), ".SAFE", EOSafeStore, 0,),
        # (lazy_fixture("S1_IW_GRD_ZIP"), lazy_fixture("S1_IW_GRD_MAPPING"), ".zarr", EOZarrStore, 0,),
        # (lazy_fixture("S1_IW_GRD_ZIP"), lazy_fixture("S1_IW_GRD_MAPPING"), ".cog", EOCogStore, 0,),
        # (lazy_fixture("S1_IW_GRD_ZIP"), lazy_fixture("S1_IW_GRD_MAPPING"), ".nc", EONetCDFStore, 0,),

        # ---> S1/Level 2 - tests
        # not working 04.07 -> SEE ISSUE #71
        # (lazy_fixture("S1_IM_OCN_ZIP"), lazy_fixture("S1_IM_OCN_MAPPING"), ".SAFE", EOSafeStore, 0),
        # (lazy_fixture("S1_IM_OCN_ZIP"), lazy_fixture("S1_IM_OCN_MAPPING"), ".zarr", EOZarrStore, 0),
        # (lazy_fixture("S1_IM_OCN_ZIP"), lazy_fixture("S1_IM_OCN_MAPPING"), ".cog", EOCogStore, 0),
        # (lazy_fixture("S1_IM_OCN_ZIP"), lazy_fixture("S1_IM_OCN_MAPPING"), ".nc", EONetCDFStore, 0),

        # ####################### S2 product type conversions ########################
        # ---> S2/Level 0 - tests -> NO PRODUCTS AVAILABLE
        # ---> S2/Level 1 - tests
        # not working 04.07 -> SEE ISSUE #76
        # S2_MSIL1C - UNZIP Format -  product type conversions
        # (lazy_fixture("S2_MSIL1C"), lazy_fixture("S2_MSIL1C_MAPPING"), ".SAFE", EOSafeStore, 156),
        # (lazy_fixture("S2_MSIL1C"), lazy_fixture("S2_MSIL1C_MAPPING"), ".zarr", EOZarrStore, 156),
        # (lazy_fixture("S2_MSIL1C"), lazy_fixture("S2_MSIL1C_MAPPING"), ".cog", EOCogStore, 156),
        # (lazy_fixture("S2_MSIL1C"), lazy_fixture("S2_MSIL1C_MAPPING"), ".nc", EONetCDFStore, 156),
        # # S2_MSIL1C - ZIP Format - product type conversions
        # (lazy_fixture("S2_MSIL1C_ZIP"), lazy_fixture("S2_MSIL1C_MAPPING"), ".SAFE", EOSafeStore, 156),
        # (lazy_fixture("S2_MSIL1C_ZIP"), lazy_fixture("S2_MSIL1C_MAPPING"), ".zarr", EOZarrStore, 156),
        # (lazy_fixture("S2_MSIL1C_ZIP"), lazy_fixture("S2_MSIL1C_MAPPING"), ".cog", EOCogStore, 156),
        # (lazy_fixture("S2_MSIL1C_ZIP"), lazy_fixture("S2_MSIL1C_MAPPING"), ".nc", EONetCDFStore, 156),
        # ---> S2/Level 2 - tests -> MAPPING not available
        # S2_MSIL2A - ZIP Format - product type conversions
        # (lazy_fixture("S2_MSIL2A_ZIP"), lazy_fixture("S2_MSIL2A_MAPPING"), ".SAFE", EOSafeStore, 52,),
        # (lazy_fixture("S2_MSIL2A_ZIP"), lazy_fixture("S2_MSIL2A_MAPPING"), ".zarr", EOZarrStore, 52,),
        # (lazy_fixture("S2_MSIL2A_ZIP"), lazy_fixture("S2_MSIL2A_MAPPING"), ".cog", EOCogStore, 0,),
        # (lazy_fixture("S2_MSIL2A_ZIP"), lazy_fixture("S2_MSIL2A_MAPPING"), ".nc", EONetCDFStore, 0,),

        # ####################### S3 product type conversions ########################
        # ---> S3/Level 0 - tests -> NO PRODUCTS AVAILABLE
        # ---> S2/Level 1/2 - tests
        # S3_OL_1 product type conversions
        # NOT WORKING -> SEE ISSUE 72
        # (lazy_fixture("S3_OL_1_EFR_ZIP"), lazy_fixture("S3_OL_1_EFR_MAPPING"), ".SEN3", EOSafeStore, 1),
        # (lazy_fixture("S3_OL_1_EFR_ZIP"), lazy_fixture("S3_OL_1_EFR_MAPPING"), ".zarr", EOZarrStore, 1),
        # (lazy_fixture("S3_OL_1_EFR_ZIP"), lazy_fixture("S3_OL_1_EFR_MAPPING"), ".cog", EOCogStore, 1),
        # (lazy_fixture("S3_OL_1_EFR_ZIP"), lazy_fixture("S3_OL_1_EFR_MAPPING"), ".nc", EONetCDFStore, 1),
        # S3_OL_2 product type conversions
        # NOT WORKING -> SEE ISSUE 73
        # (lazy_fixture("S3_OL_2_LFR_ZIP"), lazy_fixture("S3_OL_2_LFR_MAPPING"), ".SEN3", EOSafeStore, 0),
        # (lazy_fixture("S3_OL_2_LFR_ZIP"), lazy_fixture("S3_OL_2_LFR_MAPPING"), ".zarr", EOZarrStore, 0),
        # (lazy_fixture("S3_OL_2_LFR_ZIP"), lazy_fixture("S3_OL_2_LFR_MAPPING"), ".cog", EOCogStore, 0),
        # (lazy_fixture("S3_OL_2_LFR_ZIP"), lazy_fixture("S3_OL_2_LFR_MAPPING"), ".nc", EONetCDFStore, 0),
        # S3_SL_1_RBT product type conversions
        # NOT WORKING -> SEE ISSUE 74
        # (lazy_fixture("S3_SL_1_RBT_ZIP"), lazy_fixture("S3_SL_1_RBT_MAPPING"), ".SEN3", EOSafeStore, 0),
        # (lazy_fixture("S3_SL_1_RBT_ZIP"), lazy_fixture("S3_SL_1_RBT_MAPPING"), ".zarr", EOZarrStore, 0),
        # (lazy_fixture("S3_SL_1_RBT_ZIP"), lazy_fixture("S3_SL_1_RBT_MAPPING"), ".cog", EOCogStore, 0),
        # (lazy_fixture("S3_SL_1_RBT_ZIP"), lazy_fixture("S3_SL_1_RBT_MAPPING"), ".nc", EONetCDFStore, 0),
        # S3_SL_2_LST product type conversions
        # NOT WORKING -> SEE ISSUE 75
        # (lazy_fixture("S3_SL_2_LST_ZIP"), lazy_fixture("S3_SL_2_LST_MAPPING"), ".SEN3", EOSafeStore, 0),
        # (lazy_fixture("S3_SL_2_LST_ZIP"), lazy_fixture("S3_SL_2_LST_MAPPING"), ".zarr", EOZarrStore, 0),
        # (lazy_fixture("S3_SL_2_LST_ZIP"), lazy_fixture("S3_SL_2_LST_MAPPING"), ".cog", EOCogStore, 0),
        # (lazy_fixture("S3_SL_2_LST_ZIP"), lazy_fixture("S3_SL_2_LST_MAPPING"), ".nc", EONetCDFStore, 0),
        # S3_SY_2_SYN product type conversions
        # (lazy_fixture("S3_SY_2_SYN_ZIP"), lazy_fixture("S3_SY_2_SYN_MAPPING"), ".SEN3", EOSafeStore, 0),
        # (lazy_fixture("S3_SY_2_SYN_ZIP"), lazy_fixture("S3_SY_2_SYN_MAPPING"), ".zarr", EOZarrStore, 0),
        # (lazy_fixture("S3_SY_2_SYN_ZIP"), lazy_fixture("S3_SY_2_SYN_MAPPING"), ".cog", EOCogStore, 0),
        # (lazy_fixture("S3_SY_2_SYN_ZIP"), lazy_fixture("S3_SY_2_SYN_MAPPING"), ".nc", EONetCDFStore, 0),
        # S3_SR_1_SRA product type conversions -> MAPPING NOT AVAILABLE
        # (lazy_fixture("S3_SR_1_SRA_ZIP"), lazy_fixture("S3_SR_1_SRA_MAPPING"), ".SAFE", EOSafeStore, 0,),
        # (lazy_fixture("S3_SR_1_SRA_ZIP"), lazy_fixture("S3_SR_1_SRA_MAPPING"), ".zarr", EOZarrStore, 0,),
        # (lazy_fixture("S3_SR_1_SRA_ZIP"), lazy_fixture("S3_SR_1_SRA_MAPPING"), ".cog", EOCogStore, 0,),
        # (lazy_fixture("S3_SR_1_SRA_ZIP"), lazy_fixture("S3_SR_1_SRA_MAPPING"), ".nc", EONetCDFStore, 0,),
        # S3_SR_2_LAN product type conversions -> MAPPING NOT AVAILABLE
        # (lazy_fixture("S3_SR_2_LAN_ZIP"), lazy_fixture("S3_SR_2_LAN_MAPPING"), ".SAFE", EOSafeStore, 0,),
        # (lazy_fixture("S3_SR_2_LAN_ZIP"), lazy_fixture("S3_SR_2_LAN_MAPPING"), ".zarr", EOZarrStore, 0,),
        # (lazy_fixture("S3_SR_2_LAN_ZIP"), lazy_fixture("S3_SR_2_LAN_MAPPING"), ".cog", EOCogStore, 0,),
        # (lazy_fixture("S3_SR_2_LAN_ZIP"), lazy_fixture("S3_SR_2_LAN_MAPPING"), ".nc", EONetCDFStore, 0,),
        # TODO: ?? OL_1_ERR, OL_2_LRR, S3_SY_2_V10, S3_SY_2_VGP, S3_SY_2_VG1, S3_SY_2_AOD
        # fmt: on
    ],
)
def test_convert_safe_mapping(
    client,  # dask client
    input_path: str,
    mapping_filename: str,
    output_extension: str,
    output_store: type[EOProductStore],
    max_optional_miss: int,
    OUTPUT_DIR: str,
):
    impl_test_convert_safe_mapping(
        input_path,
        mapping_filename,
        output_extension,
        output_store,
        max_optional_miss,
        OUTPUT_DIR,
    )


def impl_test_convert_safe_mapping(
    input_path: str,
    mapping_filename: str,
    output_extension: str,
    output_store: type[EOProductStore],
    max_optional_miss: int,
    OUTPUT_DIR: str,
    mapping_factory=None,
):
    source_store = EOSafeStore(input_path, mapping_factory=mapping_factory)
    output_name = os.path.basename(input_path)
    # switch extension of output_name
    output_name, _ = os.path.splitext(output_name)
    output_name += output_extension
    target_store = output_store(os.path.join(OUTPUT_DIR, output_name))
    convert(source_store, target_store)
    source_product = EOProduct("", storage=source_store, mapping_factory=mapping_factory)
    target_product = EOProduct("", storage=target_store, mapping_factory=mapping_factory)

    with open(mapping_filename) as f:
        mappin_data = json.load(f)
    optional_miss = 0
    with (open_store(source_product), open_store(target_product)):
        assert source_product.product_type == mappin_data["recognition"]["product_type"]
        assert target_product.product_type == mappin_data["recognition"]["product_type"]

        for item in mappin_data["data_mapping"]:
            # TODO: should be removed after that misc was removed from mappings
            if item["item_format"] == "misc":
                continue
            data_path = item["target_path"]
            if data_path:
                try:
                    source_object = source_product[data_path]
                except KeyError as error:
                    if not item.get("is_optional", False):
                        raise error
                    optional_miss += 1
                    continue
                target_object = target_product[data_path]
            else:
                source_object = source_product
                target_object = target_product
            assert type(source_object) == type(target_object)

            if output_store != EOSafeStore or data_path not in ["", "/"]:
                # Manifest accessor set item not yet implemented
                np.testing.assert_equal(conv(source_object.attrs), conv(target_object.attrs))
            if isinstance(source_object, EOVariable):
                assert_eovariable_equal(source_object, target_object)
    assert max_optional_miss >= optional_miss


@pytest.mark.unit
@pytest.mark.parametrize(
    "mapping_filename",
    [
        # lazy_fixture("S1_IM_OCN_MAPPING"),
        # lazy_fixture("S2_MSIL1C_MAPPING"),
        # lazy_fixture("S3_OL_1_EFR_MAPPING"),
        # lazy_fixture("S3_OL_1_EFR_MAPPING"),
        # lazy_fixture("S3_OL_2_LFR_MAPPING"),
        # lazy_fixture("S3_SL_1_RBT_MAPPING"),
        # lazy_fixture("S3_SL_2_LST_MAPPING"),
        # lazy_fixture("S3_SY_2_SYN_MAPPING"),
    ],
)
def test_short_name_conflict(mapping_filename):
    short_names_dict = dict()
    with open(mapping_filename) as f:
        mapping_json = json.load(f)
        for mapping in mapping_json["data_mapping"]:
            if "short_name" not in mapping:
                continue
            assert "target_path" in mapping
            short_name = mapping["short_name"]
            if mapping["short_name"] in short_names_dict:
                assert mapping["target_path"] == short_names_dict[short_name]
            else:
                short_names_dict[short_name] = mapping["target_path"]
