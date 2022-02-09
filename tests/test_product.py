from unittest.mock import patch

import pytest
import xarray
from lxml import etree
from pyfakefs.fake_filesystem import FakeFilesystem

from eopf.exceptions import InvalidProductError, StoreNotDefinedError, StoreNotOpenError
from eopf.product.conveniences import init_product
from eopf.product.core import EOGroup, EOProduct, EOVariable

from .utils import compute_tree_structure


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
def test_product_tree(capsys):
    product = init_product("product_name")
    product.measurements.add_group("subgroup1")
    product.measurements.subgroup1.add_variable("variable1", [1, 2, 3], attrs={"name": "some name"})
    product.measurements.subgroup1.add_variable("variable2", [4, 5, 6], attrs={"name": "second variable"})
    product.tree()
    captured = capsys.readouterr()
    assert (
        captured.out
        == "├── measurements\n|  ├── subgroup1\n|    └── variable1\n|    └── variable2\n├── coordinates\n"  # noqa
    )


@pytest.mark.usecase
def test_create_a_whole_product():
    product = init_product("product_name")
    product.measurements.add_group("subgroup")
    product.measurements.subgroup.add_variable("my_variable", [[1, 2, 3, 4], [8, 9, 7, 5]])
    assert (product_length := len(product)) == len(
        product.MANDATORY_FIELD,
    ), f"number of product subgroups must be 3, current is {product_length}"
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
    tree = etree.fromstring(product._repr_html_(), parser)
    tree_structure = compute_tree_structure(tree)
    assert tree_structure == {
        "name": "product_name",
        "groups": {
            "coordinates": {"Attributes": {}},
            "measurements": {
                "Attributes": {},
                "subgroup1": {
                    "Attributes": {},
                    "variable1": {"Attributes": {"name :": "some name"}},
                    "variable2": {"Attributes": {"name :": "second variable"}},
                },
            },
        },
    }


@pytest.mark.unit
def test_generate_hierarchy_tree2():
    product = init_product("product")
    product.measurements.add_group("subgroup1")
    product.measurements.add_group("subgroup2")
    product.measurements.subgroup1.add_variable("variable11", [1, 2, 3], attrs={"name": "some name"})
    product.measurements.subgroup1.add_variable("variable12", [4, 5, 6], attrs={"name": "second variable"})
    product.measurements.subgroup2.add_variable("variable21", [1, 2, 3], attrs={"name": "value"})
    product.add_group("conditions")
    parser = etree.HTMLParser()
    tree = etree.fromstring(product._repr_html_(), parser)
    tree_structure = compute_tree_structure(tree)
    assert tree_structure == {
        "name": "product",
        "groups": {
            "coordinates": {"Attributes": {}},
            "measurements": {
                "Attributes": {},
                "subgroup1": {
                    "Attributes": {},
                    "variable11": {"Attributes": {"name :": "some name"}},
                    "variable12": {"Attributes": {"name :": "second variable"}},
                },
                "subgroup2": {"Attributes": {}, "variable21": {"Attributes": {"name :": "value"}}},
            },
            "conditions": {"Attributes": {}},
        },
    }


@pytest.mark.unit
def test_generate_hierarchy_tree3():
    product = init_product("product")
    product.measurements.add_group("subgroup1")
    product.measurements.add_group("subgroup2")
    product.measurements.subgroup1.add_variable("variable11", [1, 2, 3], attrs={"name": "some name"})
    product.measurements.subgroup1.add_variable("variable12", [4, 5, 6], attrs={"name": "second variable"})
    product.measurements.subgroup2.add_variable("variable21", [1, 2, 3], attrs={"name": "value"})
    product.add_group("conditions")
    product.conditions.add_group("subgroup3")
    product.conditions.subgroup3.add_group("subsubgroup1")
    parser = etree.HTMLParser()
    tree = etree.fromstring(product._repr_html_(), parser)
    tree_structure = compute_tree_structure(tree)
    assert tree_structure == {
        "name": "product",
        "groups": {
            "coordinates": {"Attributes": {}},
            "measurements": {
                "Attributes": {},
                "subgroup1": {
                    "Attributes": {},
                    "variable11": {"Attributes": {"name :": "some name"}},
                    "variable12": {"Attributes": {"name :": "second variable"}},
                },
                "subgroup2": {"Attributes": {}, "variable21": {"Attributes": {"name :": "value"}}},
            },
            "conditions": {"Attributes": {}, "subgroup3": {"Attributes": {}, "subsubgroup1": {"Attributes": {}}}},
        },
    }


@pytest.mark.unit
def test_eovariable_plot():
    product = init_product("product_name")
    product.measurements.add_group("subgroup")
    product.measurements.subgroup.add_variable("my_variable", [[1, 2, 3, 4], [8, 9, 7, 5]])
    product.measurements.add_variable("demo", data=[])
    product.measurements.subgroup.add_group("another_group")
    product.measurements.subgroup.another_group.add_variable("my_variable", [[1, 2, 3, 4], [8, 9, 7, 5]])

    with patch.object(xarray.DataArray, "plot", return_value=None) as mock_method:
        variable = product.measurements.demo
        variable.plot(yincrease=False)

    mock_method.assert_called_once_with(yincrease=False)
