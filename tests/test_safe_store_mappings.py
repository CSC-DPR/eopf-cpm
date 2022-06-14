import json
import os
from typing import Callable

import numpy as np
import pytest
from pytest_lazyfixture import lazy_fixture

from eopf.product.conveniences import open_store
from eopf.product.core import EOObject, EOProduct, EOVariable
from eopf.product.store import EOCogStore, EONetCDFStore, EOProductStore, EOZarrStore
from eopf.product.store.conveniences import convert
from eopf.product.store.safe import EOSafeStore
from eopf.product.utils import conv
from tests.utils import assert_eovariable_equal


@pytest.mark.need_files
@pytest.mark.unit
@pytest.mark.parametrize(
    "store_type, get_key",
    [
        # At least One test per each product type and accessor used in the mapping file

        # "item_format": "filename_to_subswath",
        (lazy_fixture("S1_IM_OCN_ZIP"), "/coordinates/osw/sub_swath"),
        # "item_format": "netcdf"
        (lazy_fixture("S1_IM_OCN_ZIP"), "/coordinates/owi/owiLon"),
        # "item_format": "attribute_element_to_flag_variable",
        (lazy_fixture("S1_IM_OCN_ZIP"), "/coordinates/owi/owiPolarisationName"),
        # "item_format": "attribute_element_to_float_variable",
        (lazy_fixture("S1_IM_OCN_ZIP"), "/conditions/state_vector/sv_x"),
        # TODO: do we test "item_format": "xmlmetadata", misc ?

        # "item_format": "xmlangles",
        (lazy_fixture("S2_MSIL1C_ZIP"), "/conditions/geometry/saa"),
        # "item_format": "jp2",
        (lazy_fixture("S2_MSIL1C_ZIP"), "/quality/msk_detfoo_b11"),
        # "item_format": "xmltp",
        (lazy_fixture("S2_MSIL1C_ZIP"), "/coordinates/tiepoint_grid/y_tp"),
        # "item_format": "grib",
        (lazy_fixture("S2_MSIL1C_ZIP"), "/coordinates/meteo/latitude_meteo"),
        # "item_format": "xmlangles",
        (lazy_fixture("S2_MSIL1C_ZIP"), "/conditions/geometry/saa"),
        # "item_format": "xmlangles",
        (lazy_fixture("S2_MSIL1C_ZIP"), "/conditions/geometry/saa"),

        # "item_format": "netcdf"
        (lazy_fixture("S3_SY_2_SYN_ZIP"), "/coordinates/image_grid/longitude"),
        (lazy_fixture("S3_SY_2_SYN_ZIP"), "/conditions/geometry/saa"),
        (lazy_fixture("S3_SY_2_SYN_ZIP"), "/measurements/olci/sdr_oa01"),

        # "item_format": "netcdf"
        (lazy_fixture("S3_OLCI_1_EFR_ZIP"), "/coordinates/image_grid/longitude"),
        (lazy_fixture("S3_OLCI_1_EFR_ZIP"), "/conditions/geometry/oza"),
        (lazy_fixture("S3_OLCI_1_EFR_ZIP"), "/measurements/orphans/oa21_radiance"),

        # "item_format": "netcdf"
        (lazy_fixture("S3_OLCI_2_LFR_ZIP"), "/coordinates/image_grid/latitude"),
        (lazy_fixture("S3_OLCI_2_LFR_ZIP"), "/conditions/geometry/oaa"),
        (lazy_fixture("S3_OLCI_2_LFR_ZIP"), "/measurements/image/rc681"),

        # "item_format": "netcdf"
        (lazy_fixture("S3_SL_1_RBT_ZIP"), "/coordinates/an/x_an"),
        (lazy_fixture("S3_SL_1_RBT_ZIP"), "/conditions/geometry_tn/sat_azimuth_tn"),
        (lazy_fixture("S3_SL_1_RBT_ZIP"), "/measurements/an/s1_radiance_an"),

        # "item_format": "netcdf"
        (lazy_fixture("S3_SL_2_LST_ZIP"), "/coordinates/in/latitude_in"),
        (lazy_fixture("S3_SL_2_LST_ZIP"), "/conditions/orphan/elevation_orphan_in"),
        (lazy_fixture("S3_SL_2_LST_ZIP"), "/measurements/in/LST"),

    ],
)
def test_read_product(dask_client_all, store_type, get_key):
    store = EOSafeStore(store_type)
    product = EOProduct("my_product", storage=store)
    product.open()
    eoval = product[get_key]
    assert isinstance(eoval, EOObject)
    assert isinstance(product.attrs, dict)
    product.store.close()


@pytest.mark.need_files
@pytest.mark.integration
@pytest.mark.parametrize(
    "store_type",
    [
        lazy_fixture("S3_OLCI_1_EFR_ZIP"),
        lazy_fixture("S3_OLCI_2_LFR_ZIP"),
        lazy_fixture("S3_SL_1_RBT_ZIP"),
        lazy_fixture("S3_SL_2_LST_ZIP"),
        lazy_fixture("S3_SY_2_SYN_ZIP"),
        lazy_fixture("S2_MSIL1C_ZIP"),
        lazy_fixture("S2_MSIL1C"),
        lazy_fixture("S1_IM_OCN_ZIP"),
    ],
)
def test_load_product(dask_client_all, store_type):
    store = EOSafeStore(store_type)
    product = EOProduct("my_product", storage=store)
    product.open()
    assert isinstance(product.attrs, dict)
    product.load()
    assert len(product._groups) > 0
    assert isinstance(product.attrs, dict)
    product.store.close()


@pytest.mark.need_files
@pytest.mark.integration
@pytest.mark.parametrize(
    "input_path, mapping_filename, output_formatter, output_store, expected_optional_miss",
    [
        # S3_OLCI_1 product type conversions
        (
            lazy_fixture("S3_OLCI_1_EFR_ZIP"),
            lazy_fixture("S3_OLCI_1_MAPPING"),
            lambda name: f"{name.replace('.zip', '.SEN3')}",
            EOSafeStore,
            0,
        ),
        (
            lazy_fixture("S3_OLCI_1_EFR_ZIP"),
            lazy_fixture("S3_OLCI_1_MAPPING"),
            lambda name: f"{name.replace('.zip', '.zarr')}",
            EOZarrStore,
            0,
        ),
        (
            lazy_fixture("S3_OLCI_1_EFR_ZIP"),
            lazy_fixture("S3_OLCI_1_MAPPING"),
            lambda name: f"{name.replace('.zip', '.cog')}",
            EOCogStore,
            0,
        ),
        (
            lazy_fixture("S3_OLCI_1_EFR_ZIP"),
            lazy_fixture("S3_OLCI_1_MAPPING"),
            lambda name: f"{name.replace('.zip', '.nc')}",
            EONetCDFStore,
            0,
        ),

        # S3_OLCI_2 product type conversions
        (
            lazy_fixture("S3_OLCI_2_LFR_ZIP"),
            lazy_fixture("S3_OLCI_2_MAPPING"),
            lambda name: f"{name.replace('.zip', '.SEN3')}",
            EOSafeStore,
            0,
        ),
        (
            lazy_fixture("S3_OLCI_2_LFR_ZIP"),
            lazy_fixture("S3_OLCI_2_MAPPING"),
            lambda name: f"{name.replace('.zip', '.zarr')}",
            EOZarrStore,
            0,
        ),
        (
            lazy_fixture("S3_OLCI_2_LFR_ZIP"),
            lazy_fixture("S3_OLCI_2_MAPPING"),
            lambda name: f"{name.replace('.zip', '.cog')}",
            EOCogStore,
            0,
        ),
        (
            lazy_fixture("S3_OLCI_2_LFR_ZIP"),
            lazy_fixture("S3_OLCI_2_MAPPING"),
            lambda name: f"{name.replace('.zip', '.nc')}",
            EONetCDFStore,
            0,
        ),

        # S3_SL_1_RBT product type conversions
        (
            lazy_fixture("S3_SL_1_RBT_ZIP"),
            lazy_fixture("S3_SL_1_RBT_MAPPING"),
            lambda name: f"{name.replace('.zip', '.SEN3')}",
            EOSafeStore,
            0,
        ),
        (
            lazy_fixture("S3_SL_1_RBT_ZIP"),
            lazy_fixture("S3_SL_1_RBT_MAPPING"),
            lambda name: f"{name.replace('.zip', '.zarr')}",
            EOZarrStore,
            0,
        ),
        (
            lazy_fixture("S3_SL_1_RBT_ZIP"),
            lazy_fixture("S3_SL_1_RBT_MAPPING"),
            lambda name: f"{name.replace('.zip', '.cog')}",
            EOCogStore,
            0,
        ),
        (
            lazy_fixture("S3_SL_1_RBT_ZIP"),
            lazy_fixture("S3_SL_1_RBT_MAPPING"),
            lambda name: f"{name.replace('.zip', '.nc')}",
            EONetCDFStore,
            0,
        ),

        # S3_SL_2_LST product type conversions
        (
            lazy_fixture("S3_SL_2_LST_ZIP"),
            lazy_fixture("S3_SL_2_LST_MAPPING"),
            lambda name: f"{name.replace('.zip', '.SEN3')}",
            EOSafeStore,
            0,
        ),
        (
            lazy_fixture("S3_SL_2_LST_ZIP"),
            lazy_fixture("S3_SL_2_LST_MAPPING"),
            lambda name: f"{name.replace('.zip', '.zarr')}",
            EOZarrStore,
            0,
        ),
        (
            lazy_fixture("S3_SL_2_LST_ZIP"),
            lazy_fixture("S3_SL_2_LST_MAPPING"),
            lambda name: f"{name.replace('.zip', '.cog')}",
            EOCogStore,
            0,
        ),
        (
            lazy_fixture("S3_SL_2_LST_ZIP"),
            lazy_fixture("S3_SL_2_LST_MAPPING"),
            lambda name: f"{name.replace('.zip', '.nc')}",
            EONetCDFStore,
            0,
        ),

        # S3_SY_2_SYN product type conversions
        (
            lazy_fixture("S3_SY_2_SYN_ZIP"),
            lazy_fixture("S3_SY_2_SYN_MAPPING"),
            lambda name: f"{name.replace('.zip', '.SEN3')}",
            EOSafeStore,
            0,
        ),
        (
            lazy_fixture("S3_SY_2_SYN_ZIP"),
            lazy_fixture("S3_SY_2_SYN_MAPPING"),
            lambda name: f"{name.replace('.zip', '.zarr')}",
            EOZarrStore,
            0,
        ),
        (
            lazy_fixture("S3_SY_2_SYN_ZIP"),
            lazy_fixture("S3_SY_2_SYN_MAPPING"),
            lambda name: f"{name.replace('.zip', '.cog')}",
            EOCogStore,
            0,
        ),
        (
            lazy_fixture("S3_SY_2_SYN_ZIP"),
            lazy_fixture("S3_SY_2_SYN_MAPPING"),
            lambda name: f"{name.replace('.zip', '.nc')}",
            EONetCDFStore,
            0,
        ),

        # S2_MSIL1C - UNZIP Format -  product type conversions -
        (
            lazy_fixture("S2_MSIL1C"),
            lazy_fixture("S2_MSIL1C_MAPPING"),
            lambda name: f"{name.join('.SEN3')}",
            EOSafeStore,
            52,
        ),
        (
            lazy_fixture("S2_MSIL1C"),
            lazy_fixture("S2_MSIL1C_MAPPING"),
            lambda name: f"{name.replace('.zip', '.zarr')}",
            EOZarrStore,
            52,
        ),
        (
            lazy_fixture("S2_MSIL1C"),
            lazy_fixture("S2_MSIL1C_MAPPING"),
            lambda name: f"{name.replace('.zip', '.cog')}",
            EOCogStore,
            0,
        ),
        (
            lazy_fixture("S2_MSIL1C"),
            lazy_fixture("S2_MSIL1C_MAPPING"),
            lambda name: f"{name.replace('.zip', '.nc')}",
            EONetCDFStore,
            0,
        ),

        # S2_MSIL1C product type conversions - ZIP Format
        (
            lazy_fixture("S2_MSIL1C_ZIP"),
            lazy_fixture("S2_MSIL1C_MAPPING"),
            lambda name: f"{name.replace('.zip', '.SEN3')}",
            EOSafeStore,
            52,
        ),
        (
            lazy_fixture("S2_MSIL1C_ZIP"),
            lazy_fixture("S2_MSIL1C_MAPPING"),
            lambda name: f"{name.replace('.zip', '.zarr')}",
            EOZarrStore,
            52,
        ),
        (
            lazy_fixture("S2_MSIL1C_ZIP"),
            lazy_fixture("S2_MSIL1C_MAPPING"),
            lambda name: f"{name.replace('.zip', '.cog')}",
            EOCogStore,
            0,
        ),
        (
            lazy_fixture("S2_MSIL1C_ZIP"),
            lazy_fixture("S2_MSIL1C_MAPPING"),
            lambda name: f"{name.replace('.zip', '.nc')}",
            EONetCDFStore,
            0,
        ),

        # S1_IM_OCN product type conversions
        (
            lazy_fixture("S1_IM_OCN_ZIP"),
            lazy_fixture("S1_IM_OCN_MAPPING"),
            lambda name: f"{name.replace('.zip', '.zarr')}",
            EOZarrStore,
            0,
        ),
        (
            lazy_fixture("S1_IM_OCN_ZIP"),
            lazy_fixture("S1_IM_OCN_MAPPING"),
            lambda name: f"{name.replace('.zip', '.cog')}",
            EOCogStore,
            0,
        ),
        (
            lazy_fixture("S1_IM_OCN_ZIP"),
            lazy_fixture("S1_IM_OCN_MAPPING"),
            lambda name: f"{name.replace('.zip', '.nc')}",
            EONetCDFStore,
            0,
        ),

    ],
)
def test_convert_safe_mapping(
    dask_client_all,
    input_path: str,
    mapping_filename: str,
    output_formatter: Callable,
    output_store: type[EOProductStore],
    expected_optional_miss: int,
    OUTPUT_DIR: str,
):
    source_store = EOSafeStore(input_path)
    name = os.path.basename(input_path)
    target_store = output_store(os.path.join(OUTPUT_DIR, output_formatter(name)))
    convert(source_store, target_store)
    source_product = EOProduct("", storage=source_store)
    target_product = EOProduct("", storage=target_store)

    with open(mapping_filename) as f:
        mappin_data = json.load(f)
    optional_miss = 0
    with (open_store(source_product), open_store(target_product)):
        assert source_product.type == mappin_data["recognition"]["product_type"]
        assert target_product.type == mappin_data["recognition"]["product_type"]

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
    assert expected_optional_miss == optional_miss
