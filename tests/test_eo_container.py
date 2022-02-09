from typing import Any, Iterable, Iterator, Optional

import fsspec
import numpy as np
import pytest
import xarray

from eopf.exceptions import (
    EOObjectExistError,
    EOObjectMultipleParentError,
    InvalidProductError,
)
from eopf.product.conveniences import init_product
from eopf.product.core import EOGroup, EOProduct, EOVariable
from eopf.product.core.eo_container import EOContainer
from eopf.product.store import EOProductStore
from eopf.product.utils import upsplit_eo_path


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

    def add_group(self, name: str, relative_path: list[str] = []) -> None:
        pass

    def add_variables(self, name: str, dataset: xarray.Dataset, relative_path: Iterable[str] = []) -> None:
        pass

    def iter(self, path: str) -> Iterator[str]:
        return iter([])

    def get_data(self, key: str) -> tuple:
        raise KeyError()


def fill_test_product() -> EOProduct:
    product: EOProduct = init_product("product_name", store_or_path_url=EmptyTestStore(""))
    product.open(mode="r")
    product.add_group("measurements/group1", coords={"c1": [2], "c2": [3], "group2": [4]})
    product.add_group("group0")

    product.measurements["group1"].add_variable("variable_a")

    product["measurements/group1"].add_variable("group2/variable_b")
    product["measurements/group1"]["group2"].add_variable("/measurements/group1/group2/variable_c", dims=["c1"])
    product.measurements.group1.group2.assign_dims(["c1"])
    product.add_variable("measurements/group1/group2/variable_d")

    product["measurements"]["group1"].add_group("group2b")
    product.measurements["group1"].add_group("/measurements/group1/group2b/group3")
    product.add_group("measurements/group1/group2b/group3b")

    return product


def assert_contain(container: EOContainer, path: str, expect_type, path_offset="/"):
    obj = container[path]
    assert obj.path == path_offset + path
    assert obj.name == path.rpartition("/")[2]
    assert isinstance(obj, expect_type)


@pytest.mark.unit
def test_add_group_var():
    product = fill_test_product()
    assert_contain(product, "group0", EOGroup)
    assert_contain(product, "measurements/group1", EOGroup)
    assert_contain(product, "measurements/group1/group2", EOGroup)
    assert_contain(product, "measurements/group1/group2b", EOGroup)
    assert_contain(product, "measurements/group1/group2b/group3b", EOGroup)
    assert_contain(product, "measurements/group1/variable_a", EOVariable)
    assert_contain(product, "measurements/group1/group2/variable_b", EOVariable)
    assert_contain(product, "measurements/group1/group2/variable_c", EOVariable)
    assert_contain(product, "measurements/group1/group2/variable_d", EOVariable)

    with pytest.raises(KeyError):
        assert_contain(product, "measurements/group2", EOGroup)
    with pytest.raises(KeyError):
        assert_contain(product, "measurements/group1/variable_d", EOVariable)


@pytest.mark.unit
def test_get_getattr():
    product = fill_test_product()

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
def test_invalids_add():
    product = fill_test_product()
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
def test_coordinates():
    product = fill_test_product()
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
def test_attributes():
    test_dict = {"a": 344, "b/c": None, "b": EOGroup}
    product = fill_test_product()
    attr = product["measurements"].attrs
    attr.update(test_dict)
    assert test_dict == product["measurements"].attrs

    attr = product["measurements/group1/variable_a"].attrs
    attr.update(test_dict)
    assert test_dict == product["measurements/group1/variable_a"].attrs


@pytest.mark.unit
def test_setitem():
    product = fill_test_product()

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
def test_multipleparent_setitem():
    product = fill_test_product()
    product_bis = fill_test_product()
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
def test_delitem():
    product = fill_test_product()
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
    product2 = fill_test_product()

    with pytest.raises(KeyError):
        del product2.measurements["/measurements/group1"]
    del product2.measurements["group1"]

    with pytest.raises(KeyError):
        product["measurements/group1/group2"]


@pytest.mark.unit
def test_invalid_dataset():
    with pytest.raises(TypeError):
        EOGroup(dataset=xarray.Dataset({1: ("a", np.array([3]))}))
    EOGroup(dataset=xarray.Dataset({"1": ("a", np.array([3]))}))
