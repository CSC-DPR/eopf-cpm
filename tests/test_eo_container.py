from typing import Any, Iterable, Iterator, MutableMapping, Optional
from unittest.mock import patch

import fsspec
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
from eopf.product.store import EOProductStore
from eopf.product.utils import upsplit_eo_path

from .utils import assert_contain, compute_tree_structure


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
        pass

    def __delitem__(self, v: Any) -> None:
        raise KeyError()

    @property
    def map(self) -> fsspec.FSMap:
        raise KeyError()

    def listdir(self, path: Optional[str] = None) -> Iterable[str]:
        return iter([])

    def rmdir(self, path: Optional[str] = None) -> None:
        raise KeyError()

    def clear(self) -> None:
        pass

    def getsize(self, path: Optional[str] = None) -> None:
        raise NotImplementedError()

    def dir_path(self, path: Optional[str] = None) -> None:
        raise NotImplementedError()

    def is_group(self, path: str) -> bool:
        raise KeyError()

    def is_variable(self, path: str) -> bool:
        raise KeyError()

    def add_group(self, name: str, relative_path: list[str] = [], attrs: dict[str, Any] = {}) -> None:
        pass

    def add_variables(self, name: str, dataset: xarray.Dataset, relative_path: Iterable[str] = []) -> None:
        pass

    def iter(self, path: str) -> Iterator[str]:
        return iter([])

    def get_data(self, key: str) -> tuple:
        raise KeyError()

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = ...):
        return super().write_attrs(group_path, attrs)

    def delete_attr(self, group_path: str, attr_name: str):
        return super().delete_attr(group_path, attr_name)


@pytest.fixture
def product() -> EOProduct:
    product: EOProduct = init_product("product_name", store_or_path_url=EmptyTestStore(""))
    with product.open(mode="w"):
        product.add_group("measurements/group1", coords={"c1": [2], "c2": [3], "group2": [4]})
        product.add_group("group0")

        product.measurements["group1"].add_variable("variable_a")

        product["measurements/group1"].add_variable("group2/variable_b")
        product["measurements/group1"]["group2"].add_variable("/measurements/group1/group2/variable_c", dims=["c1"])
        product.measurements.group1.group2.assign_dims(["c1"])

        product.measurements.add_variable("group3/variable_e")
        product.measurements.add_variable("group3/sgroup3/variable_f")

        product.add_variable("measurements/group1/group2/variable_d")

        product["measurements"]["group1"].add_group("group2b")
        product.measurements["group1"].add_group("/measurements/group1/group2b/group3")
        product.add_group("measurements/group1/group2b/group3b")

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

    with (
        patch.object(EmptyTestStore, "iter", return_value=iter(["a", "b"])) as iter_method,
        patch.object(EmptyTestStore, "get_data", return_value=(None, {})),
        product.open(mode="r"),
    ):
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
    paths = [
        "measurements",
        "measurements/group1",
        "measurements/group1/group2",
        "measurements/group1/group2/variable_c",
    ]
    product.measurements.assign_dims(["c1"])

    c1 = {p: product[p].coordinates["/c1"] for p in paths}
    c2 = product.measurements.group1.coordinates["/c2"]

    assert set(product["measurements/group1"].coordinates.keys()) == {"/c1", "/c2", "/group2"}
    assert np.all(product.coordinates[key] == value for key, value in product.measurements.group1.coordinates.items())
    for p in paths:
        container = product[p] if p != "/" else product
        assert container.get_coordinate("c1") == c1[p]
        coord = container.get_coordinate("c2")
        assert coord.path == c2.path
        assert coord == c2
        if "group1" not in p:
            with pytest.raises(KeyError):
                container.coordinates["c2"]


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
        EOGroup("group1b", None, dataset=["a", "b"])

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
    product["measurements/group1h"] = EOGroup("group1h", product, ["/", "measurements"])
    product["measurements/group1/variable_v1"] = EOVariable("variable_v1", None)
    product["/measurements/group1/variable_v2"] = EOVariable("variable_v1", None)
    product["measurements/group1"]["variable_v3"] = EOVariable("variable_v3", None)
    product["measurements/group1"]["variable_v3"] = EOVariable("variable_v3", None)
    product["measurements/group1"]["/measurements/group1/variable_v4"] = EOVariable("variable_v4", None, product)
    product["measurements/group1"]["variable_v5"] = EOVariable(
        "variable_v5",
        [1, 2],
        product,
        ["/", "measurements", "group1"],
    )
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
    np.testing.assert_equal(product["measurements/group1"]["variable_v5"], [1, 2])


@pytest.mark.unit
def test_multipleparent_setitem(product):
    product_bis = EOProduct("fake")
    with pytest.raises(EOObjectMultipleParentError):
        product["group1g"] = EOGroup("group1g", product, ("/", "measurements", "group1b"))
    with pytest.raises(EOObjectMultipleParentError):
        product["group1h"] = EOGroup("group1i", product_bis)
    with pytest.raises(EOObjectMultipleParentError):
        product["group1i"] = EOGroup("group1i", product_bis, ("measurements", "group1i"))
    with pytest.raises(EOObjectMultipleParentError):
        var = EOVariable("variable_v6", None, product_bis)
        product["measurements/group1"]["variable_v6"] = var
    with pytest.raises(EOObjectMultipleParentError):
        product["measurements/group1"]["variable_v7"] = EOVariable(
            "variable_v7",
            None,
            product_bis,
            ["/", "measurements", "group1"],
        )

    with pytest.raises(EOObjectMultipleParentError):
        product["measurements/group1"]["variable_v8"] = EOVariable(
            "variable_v7",
            None,
            product,
            ["/", "measurements", "group2"],
        )


@pytest.mark.unit
def test_delitem(product):
    del product["group0"]
    with pytest.raises(KeyError):
        product["group0"]
    product["measurements"]
    product["measurements/group1/group2/variable_b"]

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

    with (
        patch.object(EmptyTestStore, "__delitem__", return_value=None) as del_method,
        patch.object(EmptyTestStore, "__contains__", return_value=True) as in_method,
    ):
        del product["false_key"]
    del_method.assert_called_once()
    in_method.assert_called_once()


@pytest.mark.unit
def test_invalid_dataset():
    with pytest.raises(TypeError):
        EOGroup(dataset=xarray.Dataset({1: ("a", np.array([3]))}))
    EOGroup(dataset=xarray.Dataset({"1": ("a", np.array([3]))}))


@pytest.mark.unit
def test_create_product_on_memory(fs: FakeFilesystem):
    product = init_product("product_name")

    with pytest.raises(StoreNotDefinedError):
        product.open(mode="w")

    with pytest.raises(StoreNotDefinedError):
        product._relative_key("a key")

    assert product._store is None, "store must be None"
    assert not (fs.isdir(product.name) or fs.isfile(product.name)), "Product must not create any thing on fs"


@pytest.mark.unit
def test_write_product(product):

    with pytest.raises(StoreNotOpenError):
        product.write()

    with (
        patch.object(EmptyTestStore, "add_group", return_value=None) as mock_method,
        patch.object(EmptyTestStore, "add_variables", return_value=None) as mock_method2,
        product.open(mode="w"),
    ):
        product.write()

    assert mock_method2.call_count == 5
    assert mock_method.call_count == 10
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

    with (patch.object(EmptyTestStore, "get_data", return_value=(None, {})) as mock_method, product.open(mode="r")):
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


@pytest.mark.unit
def test_hierarchy_html(product):
    parser = etree.HTMLParser()
    tree = etree.fromstring(product._repr_html_(), parser)
    tree_structure = compute_tree_structure(tree)
    assert tree_structure == {
        "name": "product_name",
        "groups": {
            "coordinates": {
                "c1": {"Attributes": {}, "Dimensions": "", "Coordinates": ""},
                "c2": {"Attributes": {}, "Dimensions": "", "Coordinates": ""},
                "group2": {"Attributes": {}, "Dimensions": "", "Coordinates": ""},
            },
            "measurements": {
                "group1": {
                    "Attributes": {"_EOPF_DIMENSIONS_PATHS :": "['', '', '']"},
                    "group2": {
                        "Attributes": {"_EOPF_DIMENSIONS_PATHS :": "['']"},
                        "variable_b": {"Attributes": {}, "Dimensions": "", "Coordinates": ""},
                        "variable_c": {
                            "Attributes": {"_EOPF_DIMENSIONS_PATHS :": "['']"},
                            "Dimensions": "",
                            "Coordinates": " /->coordinates -> c1])",
                        },
                        "variable_d": {"Attributes": {}, "Dimensions": "", "Coordinates": ""},
                    },
                    "group2b": {"group3": {}, "group3b": {}},
                    "variable_a": {"Attributes": {}, "Dimensions": "", "Coordinates": ""},
                },
                "group3": {
                    "sgroup3": {"variable_f": {"Attributes": {}, "Dimensions": "", "Coordinates": ""}},
                    "variable_e": {"Attributes": {}, "Dimensions": "", "Coordinates": ""},
                },
            },
            "group0": {},
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
