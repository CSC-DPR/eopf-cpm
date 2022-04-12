import os
from typing import Callable
from unittest.mock import patch

import pytest
from pytest_lazyfixture import lazy_fixture

from eopf.product.core import EOGroup, EOProduct
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
    [lazy_fixture("S2A_MSIL1C"), lazy_fixture("S3_OLCI_L1_EFR"), lazy_fixture("S2A_MSIL1C_ZIP")],
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
    "store_type, output_formatter",
    [(lazy_fixture("S3_OLCI_L1_EFR"), lambda name: f"{name.replace('.zip', '.SEN3')}")],
)
def test_load_write_product(store_type: str, output_formatter: Callable, OUTPUT_DIR: str):
    """Test if a product can be read from one place and write to another

    Parameters
    ----------
    store_type: str
        absolute path to an existing input file
    ouput_formatter: callable
        callable that convert the input filename to the output filename
    """
    source_store = EOSafeStore(store_type)
    name = os.path.basename(store_type)
    target_store = EOSafeStore(os.path.join(OUTPUT_DIR, output_formatter(name)))
    convert(source_store, target_store)
