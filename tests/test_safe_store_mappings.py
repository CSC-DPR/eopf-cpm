from pathlib import Path

import pytest

from eopf.product import EOProduct
from eopf.product.store.mapping_factory import MappingFactory
from eopf.product.store.safe import EOSafeStore

copy_target = (
    "../data_out/S3A_OL_1_EFR____20200101T101517_20200101T101817_20200102T141102_0179_053_179_2520_LN1_O_NT_002.SEN3"
)
store_path = (
    "../data/S3A_OL_1_EFR____20200101T101517_20200101T101817_20200102T141102_0179_053_179_2520_LN1_O_NT_002.SEN3"
)


@pytest.mark.usecase
def test_read_product():
    product = EOProduct("my_product", store_or_path_url=EOSafeStore(store_path))
    product.open()
    product["/coordinates/image_grid/longitude"]
    assert "CF" in product.attrs
    product.store.close()


@pytest.mark.usecase
def test_load_product():
    product = EOProduct("my_product", store_or_path_url=EOSafeStore(store_path))
    product.open()
    assert "CF" in product.attrs
    product.load()
    assert product._groups
    assert "CF" in product.attrs
    product.store.close()


@pytest.mark.usecase
def test_load_write_product():
    product = EOProduct("my_product", store_or_path_url=EOSafeStore(store_path))
    product.open()
    product.load()
    product.store.close()
    product.open(mode="w", store_or_path_url=EOSafeStore(copy_target))
    product.write()
    product.store.close()


@pytest.mark.usecase
def test_load_product_custom_json():
    mapping_factory = MappingFactory(False)
    mapping_factory.register_mapping(str(Path(__file__).parent / "data/test_safe_mapping.json"))
    product = EOProduct("my_product", store_or_path_url=EOSafeStore(store_path, mapping_factory=mapping_factory))
    product.open()
    product.load()
    product.store.close()


@pytest.mark.usecase
def test_load_write_product_custom_json():
    mapping_factory = MappingFactory(False)
    mapping_factory.register_mapping(str(Path(__file__).parent / "data/test_safe_mapping.json"))
    product = EOProduct("my_product", store_or_path_url=EOSafeStore(store_path, mapping_factory=mapping_factory))
    product.open()
    product.load()
    product.store.close()
    product.open(mode="w", store_or_path_url=EOSafeStore(copy_target))
    product.write()
    product.store.close()
