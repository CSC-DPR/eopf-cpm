from typing import Any, Iterator, MutableMapping
from unittest.mock import patch

import numpy as np
import pytest
import xarray
from lxml import etree
from pyfakefs.fake_filesystem import FakeFilesystem

from eopf.exceptions import (
    EOObjectExistError,
    EOObjectMultipleParentError,
    InvalidProductError,
    StoreNotDefinedError,
    StoreNotOpenError,
)
from eopf.product.conveniences import init_product
from eopf.product.core import EOGroup, EOProduct, EOVariable
from eopf.product.core.eo_object import _DIMENSIONS_NAME
from eopf.product.store import EOProductStore
from eopf.product.utils import upsplit_eo_path

from .utils import assert_contain, assert_has_coords, compute_tree_structure


class EmptyTestStore(EOProductStore):
    """
    Act like an always empty store
    """

    def __iter__(self) -> Iterator[Any]:
        return iter()

    def __getitem__(self, k: Any) -> Any:
        raise KeyError()

    def __len__(self) -> int:
        return 0

    def __setitem__(self, k: Any, v: Any) -> None:
        print(k, v)
        pass

    def __delitem__(self, v: Any) -> None:
        raise KeyError()

    def is_group(self, path: str) -> bool:
        raise KeyError()

    def is_variable(self, path: str) -> bool:
        raise KeyError()

    def iter(self, path: str) -> Iterator[str]:
        return iter([])

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = ...):
        return super().write_attrs(group_path, attrs)


@pytest.fixture
def product() -> EOProduct:
    product: EOProduct = init_product("product_name", store_or_path_url=EmptyTestStore(""))
    with product.open(mode="w"):
        product.add_group(
            "measurements/group1",
            dims=("c1", "c2", "group2"),
        )
        product.add_group("group0")

        product.measurements["group1"].add_variable("variable_a")

        product["measurements/group1"].add_variable("group2/variable_b")
        product["measurements/group1"]["group2"].add_variable(
            "/measurements/group1/group2/variable_c",
            np.array([[1, 2, 3], [4, 5, 6]]),
            dims=["c1", "c2"],
        )
        product.measurements.group1.group2.assign_dims(["c1"])

        product.measurements.add_variable("group3/variable_e", attrs={"HELLO": "WORLD"})
        product.measurements.add_variable("group3/sgroup3/variable_f")

        product.add_variable("measurements/group1/group2/variable_d", [[1, 2, 3, 4], [5, 7, 4, 8]])

        product["measurements"]["group1"].add_group("group2b")
        product.measurements["group1"].add_group("/measurements/group1/group2b/group3")
        product.add_group("measurements/group1/group2b/group3b")

        product.add_variable("coordinates/coord_a", data=[1], dims=["c1"])
        product.add_variable("coordinates/coord_b", data=[1], dims=["c3"])
        product.add_variable("coordinates/g1/coord_c", data=[1], dims=["c2"])
        product.add_variable("coordinates/coord_d", data=[1], dims=["c2"])

    return product


@pytest.mark.unit
def test_product_store_is_valid():
    product = EOProduct("a_product")
    with pytest.raises(TypeError):
        product.open(mode="r", store_or_path_url=1)


@pytest.mark.unit
def test_browse_product(product):
    assert len(product.relative_path) == 0
    assert_contain(product, "group0", EOGroup)
    assert_contain(product, "measurements/group1", EOGroup)
    assert_contain(product, "measurements/group1/group2", EOGroup)
    assert_contain(product, "measurements/group1/group2b", EOGroup)
    assert_contain(product, "measurements/group1/group2b/group3b", EOGroup)
    assert_contain(product, "measurements/group1/variable_a", EOVariable)
    assert_contain(product, "measurements/group1/group2/variable_b", EOVariable)
    assert_contain(product, "measurements/group1/group2/variable_c", EOVariable)
    assert_contain(product, "measurements/group1/group2/variable_d", EOVariable)

    assert len(product) == 3

    with patch.object(EmptyTestStore, "iter", return_value=iter(["a", "b"])) as iter_method:
        with patch.object(EmptyTestStore, "__getitem__", return_value=EOGroup()):
            with product.open(mode="r"):
                assert sorted(["group0", "measurements", "coordinates", "a", "b"]) == sorted([i for i in product])
                assert product.get("a") is not None
    assert iter_method.call_count == 1

    with pytest.raises(KeyError):
        assert_contain(product, "measurements/group2", EOGroup)
    with pytest.raises(KeyError):
        assert_contain(product, "measurements/group1/variable_d", EOVariable)


@pytest.mark.unit
def test_get_getattr(product):

    assert product["measurements/group1"] is product.measurements.group1
    assert product["measurements/group1/variable_a"].path == product.measurements.group1.variable_a.path
    with pytest.raises(AttributeError):
        product.measurements.group1.variable_99
    with pytest.raises(AttributeError):
        product.measurements.groupz
    with pytest.raises(AttributeError):
        product.groupz
    with pytest.raises(AttributeError):
        product.variable_99


@pytest.mark.unit
def test_invalids_add(product):
    with pytest.raises(EOObjectExistError):
        product.add_variable("measurements/group1/group2/variable_c")
    with pytest.raises(EOObjectExistError):
        product["measurements/group1/group2"].add_variable("variable_d")
    with pytest.raises(EOObjectExistError):
        product.add_group("measurements/group1/group2b")
    with pytest.raises(EOObjectExistError):
        product.measurements["group1"].add_group("group2b")
    with pytest.raises(InvalidProductError):
        product.add_variable("direct_var")
    with pytest.raises(KeyError):
        product.add_variable("")
    with pytest.raises(KeyError):
        product.add_group("")


@pytest.mark.unit
def test_coordinates(product):

    c1_coords = ["/coordinates/coord_a"]
    c2_coords = ["/coordinates/g1/coord_c", "/coordinates/coord_d"]

    assert_has_coords(product["measurements/group1/variable_a"], [])
    assert_has_coords(product["measurements/group1"], c1_coords + c2_coords)
    assert_has_coords(product["measurements/group1/group2/variable_c"], c1_coords + c2_coords)
    assert_has_coords(product["measurements/group1/group2"], c1_coords)
    assert_has_coords(product["measurements"], [])


@pytest.mark.unit
def test_attributes(product):
    test_dict = {"a": 344, "b/c": None, "b": EOGroup}
    attr = product["measurements"].attrs
    attr.update(test_dict)
    assert test_dict == product["measurements"].attrs

    attr = product["measurements/group1/variable_a"].attrs
    attr.update(test_dict)
    assert test_dict == product["measurements/group1/variable_a"].attrs


@pytest.mark.unit
def test_setitem(product):

    with pytest.raises(TypeError):
        EOGroup("group1b", None, variables=["a", "b"])

    product["measurements/group1b/"] = EOGroup("group1b", None)

    group1c = EOGroup("group1c", None)
    with pytest.raises(InvalidProductError):
        group1c.product
    product["group1c"] = group1c
    assert group1c.product == product
    assert group1c.path == "/group1c"

    product["group1d"] = EOGroup("group1b", None)
    product["group1e"] = EOGroup("group1c", None)
    product["/measurements/"]["/measurements/group1b"] = EOGroup("group1b", None)
    product["group1f"] = EOGroup("group1f", product)
    product["measurements/group1h"] = EOGroup("group1h", product["/measurements"])

    variable_v1 = EOVariable("variable_v1", None)
    assert variable_v1.path == variable_v1.name
    product["measurements/group1/variable_v1"] = variable_v1
    assert variable_v1.path == "/measurements/group1/variable_v1"

    product["/measurements/group1/variable_v2"] = EOVariable("variable_v1", None)
    product["measurements/group1"]["variable_v3"] = EOVariable("variable_v3", None)
    product["measurements/group1"]["variable_v3"] = EOVariable("variable_v3", None)
    product["measurements/group1"]["/measurements/group1/variable_v4"] = EOVariable(
        "variable_v4",
        None,
        product["/measurements/group1"],
    )
    product["measurements/group1"]["variable_v5"] = EOVariable("variable_v5", [1, 2], product["/measurements/group1"])
    with pytest.raises(KeyError):
        product["measurements/group1"][""] = EOVariable("", None)

    path_asserts = [
        "/measurements/group1b",
        "/group1c",
        "/group1d",
        "/group1e",
        "/group1f",
        "/measurements/group1h",
        "/measurements/group1/variable_v1",
        "/measurements/group1/variable_v2",
        "/measurements/group1/variable_v3",
        "/measurements/group1/variable_v4",
    ]
    for path in path_asserts:
        assert product[path].path == path
        assert product[path].product == product
        assert product[path].name == upsplit_eo_path(path)[1]
    with pytest.raises(KeyError):
        product["measurements/group1/group2/variable_b/sub/values"] = [1, 2, 3]
    np.testing.assert_equal(product["measurements/group1"]["variable_v5"], [1, 2])


@pytest.mark.unit
def test_multipleparent_setitem(product):
    product_bis = EOProduct("fake")
    with pytest.raises(EOObjectMultipleParentError):
        product["group1g"] = EOGroup("group1g", product["/measurements/group1"])
    with pytest.raises(EOObjectMultipleParentError):
        product["group1h"] = EOGroup("group1i", product_bis)
    with pytest.raises(EOObjectMultipleParentError):
        product["group1i"] = EOGroup("group1i", product_bis)
    with pytest.raises(EOObjectMultipleParentError):
        var = EOVariable("variable_v6", None, product_bis)
        product["measurements/group1"]["variable_v6"] = var
    with pytest.raises(EOObjectMultipleParentError):
        product["measurements/group1"]["variable_v7"] = product_bis.add_variable("/measurements/group1/variable_v7")

    with pytest.raises(EOObjectMultipleParentError):
        product["measurements/group1"]["variable_v8"] = EOVariable(
            "variable_v7",
            parent=product,
        )


@pytest.mark.unit
def test_delitem(product):
    del product["group0"]
    with pytest.raises(KeyError):
        product["group0"]
    product["measurements"]
    product["measurements/group1/group2/variable_b"]

    with pytest.raises(KeyError, match="EOVariable not support item deletion"):
        del product["measurements/group1/group2/variable_b/sub/values"]

    del product["measurements/group1/group2/variable_b"]
    with pytest.raises(KeyError):
        product["measurements/group1/group2/variable_b"]
    del product["measurements/group1/"]["group2/variable_c"]

    with pytest.raises(KeyError):
        product["measurements/group1/group2/variable_b"]
    product["measurements/group1/group2"]
    del product.measurements["group1/group2b"]
    with pytest.raises(KeyError):
        product["measurements/group1/group2b"]
    del product.measurements["group1/group2"]
    with pytest.raises(KeyError):
        product["measurements/group1/group2"]

    with pytest.raises(KeyError):
        del product.measurements["/measurements/group1"]

    assert len(product.measurements.group3) > 0
    del product.measurements["group3"]
    with pytest.raises(KeyError):
        assert product.measurements["group3"]

    with pytest.raises(KeyError):
        product["measurements/group1/group2"]

    with patch.object(EmptyTestStore, "__delitem__", return_value=None) as del_method:
        with patch.object(EmptyTestStore, "__contains__", return_value=True) as in_method:
            del product["false_key"]
    del_method.assert_called_once()
    in_method.assert_called_once()


@pytest.mark.unit
def test_invalid_dataset():
    with pytest.raises(TypeError):
        EOGroup(variables={1: EOVariable(data=np.array([3]))})
    EOGroup(variables={"1": EOVariable(data=np.array([3]))})


@pytest.mark.unit
def test_create_product_on_memory(fs: FakeFilesystem):
    product = init_product("product_name")

    with pytest.raises(StoreNotDefinedError):
        product.open(mode="w")

    with pytest.raises(StoreNotDefinedError):
        product._store_key("a key")

    with pytest.raises(StoreNotDefinedError):
        product.write()

    assert product._store is None, "store must be None"
    assert not (fs.isdir(product.name) or fs.isfile(product.name)), "Product must not create any thing on fs"


@pytest.mark.unit
def test_write_product(product):

    with pytest.raises(StoreNotOpenError):
        product.write()

    with patch.object(EmptyTestStore, "__setitem__", return_value=None) as mock_method:
        with product.open(mode="w"):
            product.write()

    assert mock_method.call_count == 21
    assert product._store is not None, "store must be set"

    product._store = None
    with pytest.raises(StoreNotDefinedError):
        product.write()
    with pytest.raises(StoreNotDefinedError):
        product.measurements.write()


@pytest.mark.unit
def test_load_product(product):
    with pytest.raises(StoreNotOpenError):
        product.load()

    with patch.object(EmptyTestStore, "__getitem__", return_value=(EOGroup())) as mock_method:
        with product.open(mode="r"):
            product.load()

    assert mock_method.call_count == 1
    assert product._store is not None, "store must be set"

    product._store = None
    with pytest.raises(StoreNotDefinedError):
        product.load()


@pytest.mark.unit
def test_product_must_have_mandatory_group():
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
def test_product_tree(product, capsys):
    product.tree()
    captured = capsys.readouterr()
    assert captured.out == (
        "├── measurements\n"
        "|  ├── group1\n"
        "|    └── variable_a\n"
        "|    ├── group2\n"
        "|      └── variable_b\n"
        "|      └── variable_c\n"
        "|      └── variable_d\n"
        "|    ├── group2b\n"
        "|      ├── group3\n"
        "|      ├── group3b\n"
        "|  ├── group3\n"
        "|    └── variable_e\n"
        "|    ├── sgroup3\n"
        "|      └── variable_f\n"
        "├── coordinates\n"
        "|  └── coord_a\n"
        "|  └── coord_b\n"
        "|  └── coord_d\n"
        "|  ├── g1\n"
        "|    └── coord_c\n├── group0\n"
    )


@pytest.mark.unit
def test_hierarchy_html(product):
    tree = etree.HTML(product._repr_html_())
    tree_structure = compute_tree_structure(tree)
    dim_attr_key = _DIMENSIONS_NAME
    assert tree_structure == {
        "product_name": {
            "coordinates": {
                "coord_a": {"dims": ("c1",), "attrs": {dim_attr_key: ["c1"]}, "coords": ["/coordinates/coord_a"]},
                "coord_b": {"dims": ("c3",), "attrs": {dim_attr_key: ["c3"]}, "coords": ["/coordinates/coord_b"]},
                "coord_d": {
                    "dims": ("c2",),
                    "attrs": {dim_attr_key: ["c2"]},
                    "coords": ["/coordinates/g1/coord_c", "/coordinates/coord_d"],
                },
                "g1": {
                    "coord_c": {
                        "attrs": {"_ARRAY_DIMENSIONS": ["c2"]},
                        "coords": ["/coordinates/g1/coord_c", "/coordinates/coord_d"],
                        "dims": ("c2",),
                    },
                    "dims": (),
                    "attrs": {},
                    "coords": [],
                },
                "dims": (),
                "attrs": {},
                "coords": [],
            },
            "measurements": {
                "group1": {
                    "group2": {
                        "variable_b": {"dims": (), "attrs": {}, "coords": []},
                        "variable_c": {
                            "dims": ("c1", "c2"),
                            "attrs": {dim_attr_key: ["c1", "c2"]},
                            "coords": ["/coordinates/g1/coord_c", "/coordinates/coord_a", "/coordinates/coord_d"],
                        },
                        "variable_d": {
                            "dims": ("dim_0", "dim_1"),
                            "attrs": {dim_attr_key: ["dim_0", "dim_1"]},
                            "coords": [],
                        },
                        "dims": (),
                        "attrs": {dim_attr_key: ["c1"]},
                        "coords": [],
                    },
                    "group2b": {
                        "group3": {"dims": (), "attrs": {}, "coords": []},
                        "group3b": {"dims": (), "attrs": {}, "coords": []},
                        "dims": (),
                        "attrs": {},
                        "coords": [],
                    },
                    "variable_a": {"dims": (), "attrs": {}, "coords": []},
                    "dims": (),
                    "attrs": {dim_attr_key: ["c1", "c2", "group2"]},
                    "coords": [],
                },
                "group3": {
                    "sgroup3": {
                        "variable_f": {"dims": (), "attrs": {}, "coords": []},
                        "dims": (),
                        "attrs": {},
                        "coords": [],
                    },
                    "variable_e": {"dims": (), "attrs": {"HELLO": "WORLD"}, "coords": []},
                    "dims": (),
                    "attrs": {},
                    "coords": [],
                },
                "dims": (),
                "attrs": {},
                "coords": [],
            },
            "group0": {"dims": (), "attrs": {}, "coords": []},
            "dims": (),
            "attrs": {},
            "coords": [],
        },
    }


@pytest.mark.unit
def test_eovariable_plot(product):
    with patch.object(xarray.DataArray, "plot", return_value=None) as mock_method:
        variable = product["measurements/group1/group2/variable_d"]
        variable.plot(yincrease=False)

    mock_method.assert_called_once_with(yincrease=False)


@pytest.mark.unit
def test_group_to_product(product):
    with pytest.raises(NotImplementedError):
        product.measurements.to_product()
