import json
import os
from typing import Callable

import numpy as np
import pytest
from pytest_lazyfixture import lazy_fixture

from eopf.product.conveniences import open_store
from eopf.product.core import EOProduct, EOVariable
from eopf.product.store import EOProductStore, EOZarrStore
from eopf.product.store.conveniences import convert
from eopf.product.store.safe import EOSafeStore
from eopf.product.utils import conv
from tests.utils import assert_eovariable_equal


@pytest.mark.need_files
@pytest.mark.unit
@pytest.mark.parametrize(
    "store_type, get_key",
    [
        (lazy_fixture("S3_OLCI_L1_EFR"), "/coordinates/image_grid/longitude"),
        (lazy_fixture("S1_IM_OCN"), "/coordinates/osw/sub_swath"),
        (lazy_fixture("S1_IM_OCN"), "/coordinates/owi/owiLon"),
        (lazy_fixture("S1_IM_OCN"), "/coordinates/owi/owiPolarisationName"),
        (lazy_fixture("S1_IM_OCN"), "/conditions/state_vector/sv_x"),
        (lazy_fixture("S2A_MSIL1C"), "/conditions/geometry/saa"),
        (lazy_fixture("S2A_MSIL1C"), "/quality/msk_detfoo_b11"),
        (lazy_fixture("S2A_MSIL1C"), "/quality/msk_qualit_b11"),
        (lazy_fixture("S2A_MSIL1C"), "/measurements/reflectances_20m/b05"),
        (lazy_fixture("S2A_MSIL1C"), "/coordinates/meteo/longitude_meteo"),
        (lazy_fixture("S2A_MSIL1C"), "/coordinates/tiepoint_grid/x_tp"),
        (lazy_fixture("S2A_MSIL1C"), "/coordinates/image_grid_20m/x_20m"),
        (lazy_fixture("S2A_MSIL1C"), "/conditions/meteo/omaod550"),
    ],
)
def test_read_product(dask_client_all, store_type, get_key):
    store = EOSafeStore(store_type)
    product = EOProduct("my_product", storage=store)
    product.open()
    product[get_key]
    assert isinstance(product.attrs, dict)
    product.store.close()


@pytest.mark.need_files
@pytest.mark.integration
@pytest.mark.parametrize(
    "store_type",
    [lazy_fixture("S3_OLCI_L1_EFR")],
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
        (
            lazy_fixture("S3_OLCI_L1_EFR"),
            lazy_fixture("S3_OLCI_L1_MAPPING"),
            lambda name: f"{name.replace('.zip', '.SEN3')}",
            EOSafeStore,
            0,
        ),
        (
            lazy_fixture("S3_OLCI_L1_EFR"),
            lazy_fixture("S3_OLCI_L1_MAPPING"),
            lambda name: f"{name.replace('.zip', '.zarr')}",
            EOZarrStore,
            0,
        ),
        (
            lazy_fixture("S2A_MSIL1C"),
            lazy_fixture("S2A_MSIL1C_MAPPING"),
            lambda name: f"{name.replace('.zip', '.zarr')}",
            EOZarrStore,
            52,
        ),
        (
            lazy_fixture("S2A_MSIL1C_ZIP"),
            lazy_fixture("S2A_MSIL1C_MAPPING"),
            lambda name: f"{name.replace('.zip', '.zarr')}",
            EOZarrStore,
            52,
        ),
        # (
        #     lazy_fixture("S1_IM_OCN"),
        #     lazy_fixture("S1_IM_OCN_MAPPING"),
        #     lambda name: f"{name.replace('.zip', '.zarr')}",
        #     EOZarrStore,
        #     0,
        # ),
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
