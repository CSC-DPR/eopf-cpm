from typing import Any, Iterable, Iterator, Optional

import fsspec
import numpy as np
import pytest
import xarray

from eopf.exceptions import EOObjectExistError, InvalidProductError
from eopf.product.conveniences import init_product
from eopf.product.core import EOGroup, EOProduct, EOVariable
from eopf.product.core.eo_container import EOContainer
from eopf.product.store import EOProductStore


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


def fill_test_product() -> EOProduct:
    product: EOProduct = init_product("product_name", store_or_path_url=EmptyTestStore(""))
    product.open(mode="r")
    product.add_group("measurements/group1")
    product.add_group("group0")

    product.measurements["group1"].add_variable("variable_a")
    product["measurements/group1"].add_variable("group2/variable_b")
    product["measurements"]["group1"]["group2"].add_variable("variable_c")
    product.add_variable("measurements/group1/group2/variable_d")

    product.measurements["group1"].add_group("group2b")
    product.measurements["group1"].add_group("group2b/group3")
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


@pytest.mark.unit
def test_coordinates():
    product = fill_test_product()
    paths = {
        "/": [0],
        "measurements": [1],
        "measurements/group1": [2],
        "measurements/group1/group2": [3],
        "measurements/group1/group2/variable_c": [4],
    }
    c1 = {p: product.add_variable("coordinates/" + p + "/c1", paths[p]) for p in paths}

    c2 = product.add_variable("coordinates/measurements/group1/c2")

    expected_dict = {"c1": c1["measurements/group1"], "c2": c2}
    c2_level_keys = expected_dict.keys()
    assert set(product["coordinates/measurements/group1/"].coordinates.keys()) == {"c1", "c2", "group2"}
    assert np.all(
        product["coordinates/measurements/group1/"].coordinates[coord_name] == expected_dict[coord_name]
        for coord_name in c2_level_keys
    )
    for p in paths:
        container = product[p] if p != "/" else product
        assert container.get_coordinate("c1") == c1[p]
        if "measurements/group1" in p:
            assert container.get_coordinate("c2").path == c2.path
        else:
            with pytest.raises(KeyError):
                container.get_coordinate("c2")


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
