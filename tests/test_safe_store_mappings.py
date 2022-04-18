import os
from typing import Callable
from unittest.mock import patch

import pytest
from pytest_lazyfixture import lazy_fixture

from eopf.product.core import EOGroup, EOProduct
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
    [lazy_fixture("S3_OLCI_L1_EFR")],
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
    "input_path, output_formatter, output_store",
    [
        (lazy_fixture("S3_OLCI_L1_EFR"), lambda name: f"{name.replace('.zip', '.SEN3')}", EOSafeStore),
        (lazy_fixture("S3_OLCI_L1_EFR"), lambda name: f"{name.replace('.zip', '.zarr')}", EOZarrStore),
        (lazy_fixture("S2A_MSIL1C"), lambda name: f"{name.replace('.zip', '.zarr')}", EOZarrStore),
        (lazy_fixture("S2A_MSIL1C_ZIP"), lambda name: f"{name.replace('.zip', '.zarr')}", EOZarrStore),
    ],
)
def test_convert_safe_mapping(
    input_path: str,
    output_formatter: Callable,
    output_store: type[EOProductStore],
    OUTPUT_DIR: str,
):
    impl_test_convert_store(input_path, EOSafeStore, output_formatter, output_store, OUTPUT_DIR)


def impl_test_convert_store(
    input_path: str,
    input_store_t: type[EOProductStore],
    output_formatter: Callable,
    output_store_t: type[EOProductStore],
    output_dir: str,
):
    source_store = input_store_t(input_path)
    name = os.path.basename(input_path)
    target_store = output_store_t(os.path.join(output_dir, output_formatter(name)))
    convert(source_store, target_store)
