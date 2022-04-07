from unittest.mock import patch

import pytest

from eopf.product.core import EOGroup, EOProduct, EOVariable
from eopf.product.store.manifest import ManifestStore
from eopf.product.store.safe import EOSafeStore

from .utils import PARENT_DATA_PATH

IN_DIR = f"{PARENT_DATA_PATH}/data"
OUT_DIR = f"{PARENT_DATA_PATH}/data_out"


STORE_PATHS = {
    "S3": "S3A_OL_1_EFR____20200101T101517_20200101T101817_20200102T141102_0179_053_179_2520_LN1_O_NT_002.SEN3",
    "S2A_MSIL1C": "S2A_MSIL1C_20220314T092031_N0400_R093_T35VPH_20220314T093837.SAFE.SEN3",
}


@pytest.mark.usecase
@pytest.mark.parametrize(
    "store_type, get_key",
    [
        ("S3", "/coordinates/image_grid/longitude"),
        ("S2A_MSIL1C", "/conditions/geometry/saa"),
        ("S2A_MSIL1C", "/quality/msk_detfoo_b11"),
        ("S2A_MSIL1C", "/quality/msk_qualit_b11"),
        ("S2A_MSIL1C", "/measurements/reflectances_20m/b01"),
        ("S2A_MSIL1C", "/coordinates/meteo/longitude_meteo"),
        ("S2A_MSIL1C", "/coordinates/tiepoint_grid/x_tp"),
        ("S2A_MSIL1C", "/coordinates/image_grid_20m/x_20m"),
        ("S2A_MSIL1C", "/conditions/meteo/omaod550"),
    ],
)
def test_read_product(store_type, get_key):
    store = EOSafeStore(f"{IN_DIR}/{STORE_PATHS[store_type]}")
    product = EOProduct("my_product", store_or_path_url=store)
    product.open()
    product[get_key]
    with patch.object(ManifestStore, "__getitem__", return_value=EOGroup()) as mock_method:
        product.attrs
    mock_method.call_count == 1
    product.store.close()


@pytest.mark.usecase
@pytest.mark.parametrize(
    "store_type",
    ["S3", "S2A_MSIL1C"],
)
def test_load_product(store_type):
    store = EOSafeStore(f"{IN_DIR}/{STORE_PATHS[store_type]}")
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


@pytest.mark.usecase
@pytest.mark.parametrize(
    "store_type",
    [
        "S3",
    ],
)
def test_load_write_product(store_type):
    source_store = EOSafeStore(f"{IN_DIR}/{STORE_PATHS[store_type]}")
    target_store = EOSafeStore(f"{OUT_DIR}/{STORE_PATHS[store_type]}")
    product = EOProduct("my_product", store_or_path_url=source_store)
    product.open()
    product.load()
    product.open(mode="w", store_or_path_url=target_store)
    product.write()
    product.store.close()
