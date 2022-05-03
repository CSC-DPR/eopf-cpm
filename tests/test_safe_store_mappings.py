import json
import os
from typing import Callable
from unittest.mock import patch

import numpy as np
import pytest
from pytest_lazyfixture import lazy_fixture

from eopf.product.conveniences import open_store
from eopf.product.core import EOGroup, EOProduct, EOVariable
from eopf.product.store import EOProductStore, EOZarrStore
from eopf.product.store.conveniences import convert
from eopf.product.store.manifest import ManifestStore
from eopf.product.store.safe import EOSafeStore


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
        (lazy_fixture("S2A_MSIL1C"), "/measurements/reflectances_20m/b01"),
        (lazy_fixture("S2A_MSIL1C"), "/coordinates/meteo/longitude_meteo"),
        (lazy_fixture("S2A_MSIL1C"), "/coordinates/tiepoint_grid/x_tp"),
        (lazy_fixture("S2A_MSIL1C"), "/coordinates/image_grid_20m/x_20m"),
        (lazy_fixture("S2A_MSIL1C"), "/conditions/meteo/omaod550"),
    ],
)
def test_read_product(store_type, get_key):
    store = EOSafeStore(store_type)
    product = EOProduct("my_product", store_or_path_url=store)
    product.open()
    product[get_key]
    with patch.object(ManifestStore, "__getitem__", return_value=EOGroup()) as mock_method:
        product.attrs
    mock_method.call_count == 1
    product.store.close()


@pytest.mark.need_files
@pytest.mark.integration
@pytest.mark.parametrize(
    "store_type",
    [lazy_fixture("S3_OLCI_L1_EFR"), lazy_fixture("S1_IM_OCN")],
)
def test_load_product(store_type):
    store = EOSafeStore(store_type)
    product = EOProduct("my_product", store_or_path_url=store)
    product.open()
    with patch.object(ManifestStore, "__getitem__", return_value=EOGroup()) as mock_method:
        product.attrs
    mock_method.call_count == 1
    product.load()
    assert len(product._groups) > 0
    with patch.object(ManifestStore, "__getitem__", return_value=EOGroup()) as mock_method:
        product.attrs
    mock_method.call_count == 1
    product.store.close()


@pytest.mark.need_files
@pytest.mark.integration
@pytest.mark.parametrize(
    "input_path, mapping_filename, output_formatter, output_store",
    [
        (
            lazy_fixture("S3_OLCI_L1_EFR"),
            lazy_fixture("S3_OLCI_L1_MAPPING"),
            lambda name: f"{name.replace('.zip', '.SEN3')}",
            EOSafeStore,
        ),
        (
            lazy_fixture("S3_OLCI_L1_EFR"),
            lazy_fixture("S3_OLCI_L1_MAPPING"),
            lambda name: f"{name.replace('.zip', '.zarr')}",
            EOZarrStore,
        ),
        (
            lazy_fixture("S2A_MSIL1C"),
            lazy_fixture("S2A_MSIL1C_MAPPING"),
            lambda name: f"{name.replace('.zip', '.zarr')}",
            EOZarrStore,
        ),
        (
            lazy_fixture("S2A_MSIL1C_ZIP"),
            lazy_fixture("S2A_MSIL1C_MAPPING"),
            lambda name: f"{name.replace('.zip', '.zarr')}",
            EOZarrStore,
        ),
        (
            lazy_fixture("S1_IM_OCN"),
            lazy_fixture("S1_IM_OCN_MAPPING"),
            lambda name: f"{name.replace('.zip', '.zarr')}",
            EOZarrStore,
        ),
    ],
)
def test_convert_safe_mapping(
    input_path: str,
    mapping_filename: str,
    output_formatter: Callable,
    output_store: type[EOProductStore],
    OUTPUT_DIR: str,
):
    source_store = EOSafeStore(input_path)
    name = os.path.basename(input_path)
    target_store = output_store(os.path.join(OUTPUT_DIR, output_formatter(name)))
    convert(source_store, target_store)
    source_product = EOProduct("", store_or_path_url=source_store)
    target_product = EOProduct("", store_or_path_url=target_store)

    with open(mapping_filename) as f:
        mappin_data = json.load(f)

    with (open_store(source_product), open_store(target_product)):
        for item in mappin_data["data_mapping"]:
            # TODO: should be removed after that misc was removed from mappings
            if item["item_format"] == "misc":
                continue
            data_path = item["target_path"]
            if data_path:
                source_object = source_product[data_path]
                target_object = target_product[data_path]
            else:
                source_object = source_product
                target_object = target_product
            assert type(source_object) == type(target_object)
            np.testing.assert_equal(source_object.attrs, target_object.attrs)
            if isinstance(source_object, EOVariable):
                if source_object.is_masked:
                    source_data = np.ma.getdata(source_object.data)
                else:
                    source_data = source_object.data

                if target_object.is_masked:
                    target_data = np.ma.getdata(target_object.data)
                else:
                    target_data = target_object.data

                if source_object.is_masked and target_object.is_masked:
                    assert np.ma.allequal(source_data, target_data)
                else:
                    assert np.array_equal(source_data, target_data, equal_nan=True)
