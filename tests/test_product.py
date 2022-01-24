import pytest
from pyfakefs.fake_filesystem import FakeFilesystem

from eopf.exceptions import InvalidProductError, StoreNotDefinedError, StoreNotOpenError
from eopf.product.conveniences import init_product
from eopf.product.core import EOProduct


@pytest.mark.unit
def test_create_product_on_memory(fs: FakeFilesystem):
    product = init_product("product_name")

    assert product._store is None, "store must be None"
    assert not (fs.isdir(product.name) or fs.isfile(product.name)), "Product must not create any thing on fs"


@pytest.mark.unit
def test_create_product_on_fs(fs: FakeFilesystem):
    product = init_product("product_name", store_or_path_url="product_name")

    with product.open(mode="w"):
        product.write()
    assert product._store is not None, "store must be set"
    assert fs.isdir(product.name) or fs.isfile(product.name), "Product must create any thing on fs"


@pytest.mark.unit
def test_cannot_write_without_open(fs: FakeFilesystem):
    product = init_product("product_name", store_or_path_url="product_name")
    assert product._store is not None, "store must be set"

    with pytest.raises(StoreNotOpenError):
        product.write()

    assert not (fs.isdir(product.name) or fs.isfile(product.name)), "Product must not create any thing on fs"


@pytest.mark.unit
def test_cannot_write_without_fs(fs: FakeFilesystem):
    product = init_product("product_name")
    assert product._store is None, "store must be None"

    with pytest.raises(StoreNotDefinedError), product.open(mode="w"):
        product.write()

    assert not (fs.isdir(product.name) or fs.isfile(product.name)), "Product must not create any thing on fs"


@pytest.mark.unit
def test_product_must_have_mandatory_group(fs: FakeFilesystem):
    product = EOProduct("product_name")
    assert product._store is None, "store must be None"

    with pytest.raises(InvalidProductError):
        product.validate()
    assert not product.is_valid()

    for group in product.MANDATORY_FIELD:
        product.add_group(group)

    product.validate()
    assert product.is_valid()
