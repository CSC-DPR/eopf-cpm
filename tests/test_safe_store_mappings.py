import os
from typing import Callable
from unittest.mock import patch

import pytest
from pytest_lazyfixture import lazy_fixture

from eopf.product.core import EOGroup, EOProduct, EOVariable
from eopf.product.store.conveniences import convert
from eopf.product.store.manifest import ManifestStore
from eopf.product.store.safe import EOSafeStore


@pytest.mark.need_files
@pytest.mark.unit
@pytest.mark.parametrize(
    "store_type, get_key",
    [
        (lazy_fixture("S3_OLCI_L1_EFR"), "/coordinates/image_grid/longitude"),
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
@pytest.mark.unit
@pytest.mark.parametrize(
    "store_type",
    [
        lazy_fixture("S3_OLCI_L1_EFR"),
    ],
)
def test_load_product(store_type):
    store = EOSafeStore(store_type)
    product = EOProduct("my_product", store_or_path_url=store)
    product.open()
    with patch.object(ManifestStore, "__getitem__", return_value=EOGroup()) as mock_method:
        product.attrs
    mock_method.call_count == 1
    with (patch.object(EOVariable, "assign_dims", return_value=None),):
        product.load()
    assert len(product._groups) > 0
    with patch.object(ManifestStore, "__getitem__", return_value=EOGroup()) as mock_method:
        product.attrs
    mock_method.call_count == 1
    product.store.close()


@pytest.mark.need_files
@pytest.mark.unit
@pytest.mark.parametrize(
    "store_type, output_formatter",
    [(lazy_fixture("S3_OLCI_L1_EFR"), lambda name: f"{name.replace('.zip', '.SEN3')}")],
)
def test_load_write_product(store_type: str, output_formatter: Callable, OUTPUT_DIR: str):
    source_store = EOSafeStore(store_type)
    _, _, name = store_type.rpartition("/")
    target_store = EOSafeStore(os.path.join(OUTPUT_DIR, output_formatter(name)))
    convert(source_store, target_store)
