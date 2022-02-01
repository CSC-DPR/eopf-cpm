import pytest
from lxml import etree
from pyfakefs.fake_filesystem import FakeFilesystem

from eopf.exceptions import InvalidProductError, StoreNotDefinedError, StoreNotOpenError
from eopf.product.conveniences import init_product
from eopf.product.core import EOGroup, EOProduct, EOVariable


@pytest.mark.unit
def test_create_product_on_memory(fs: FakeFilesystem):
    product = init_product("product_name")

    assert product._store is None, "store must be None"
    assert not (fs.isdir(product.name) or fs.isfile(product.name)), "Product must not create any thing on fs"


@pytest.mark.unit
def test_write_product_on_fs(fs: FakeFilesystem):
    product = init_product("product_name", store_or_path_url="file://product_name")
    product.measurements.add_variable("a_variables")

    with product.open(mode="w"):
        product.write()
    assert product._store is not None, "store must be set"
    assert fs.isdir(product.name) or fs.isfile(product.name), "Product must create any thing on fs"


@pytest.mark.unit
def test_cannot_write_without_open(fs: FakeFilesystem):
    product = init_product("product_name", store_or_path_url="file://product_name")
    assert product._store is not None, "store must be set"

    with pytest.raises(StoreNotOpenError):
        product.write()

    assert not (fs.isdir(product.name) or fs.isfile(product.name)), "Product must not create any thing on fs"


@pytest.mark.unit
def test_cannot_write_without_fs(fs: FakeFilesystem):
    product = init_product("product_name")
    assert product._store is None, "store must be None"

    with pytest.raises(StoreNotDefinedError):
        product.open(mode="w")

    with pytest.raises(StoreNotDefinedError):
        product.write()

    with pytest.raises(StoreNotDefinedError):
        product.load()

    assert not (fs.isdir(product.name) or fs.isfile(product.name)), "Product must not create any thing on fs"

    product.add_group("a_subgroup")

    with pytest.raises(StoreNotDefinedError):
        product.a_subgroup.write()

    with pytest.raises(StoreNotDefinedError):
        product.a_subgroup._relative_key("sub_key")


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


@pytest.mark.unit
def test_add_group_hierarchy_from_path(fs: FakeFilesystem):
    product = EOProduct("product_name")
    assert product._store is None, "store must be None"

    product.add_group("a/b/c")
    assert isinstance(product.a.b.c, EOGroup)
    assert product["a/b/c"].name == "c"


@pytest.mark.usecase
def test_create_a_whole_product():
    product = init_product("product_name")
    product.measurements.add_group("subgroup")
    product.measurements.subgroup.add_variable("my_variable", [[1, 2, 3, 4], [8, 9, 7, 5]])
    assert (product_length := len(product)) == 3, f"number of product subgroups must be 3, current is {product_length}"
    assert (
        measurements_length := len(product.measurements)
    ) == 1, f"number of product.measurements subgroups must be 1, current is {measurements_length}"
    assert (
        ndimensonal := len(product.measurements.subgroup.my_variable)
    ) == 2, f"variable is a {ndimensonal} dimensional array, 2 was expected"

    for key, value in product.measurements.groups:
        assert isinstance(key, str)
        assert isinstance(value, EOGroup)
        assert value.name == key

    for key, value in product.measurements.subgroup.variables:
        assert isinstance(key, str)
        assert isinstance(value, EOVariable)
        assert value.name == key

@pytest.mark.unit
def test_generate_hierarchy_tree():
    product = init_product("product_name")
    product.measurements.add_group("subgroup1")
    product.measurements.subgroup1.add_variable("variable1", [1, 2, 3], attrs={"name": "some name"})
    product.measurements.subgroup1.add_variable("variable2", [4, 5, 6], attrs={"name": "second variable"})
    parser = etree.HTMLParser()
    tree = etree.fromstring(product.tree(), parser)
    attribute_name = tree.xpath(
        "/html/body/div/div/ul/li[2]/div/div/ul/li[2]/div/div/ul/li[2]/div/div/ul/li[1]/div/dl/dt/span/text()",
    )[0]
    assert attribute_name == "name :"
    attribute_value = tree.xpath(
        "/html/body/div/div/ul/li[2]/div/div/ul/li[2]/div/div/ul/li[2]/div/div/ul/li[1]/div/dl/dd/text()",
    )[0]
    assert attribute_value == "some name"
