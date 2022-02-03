from typing import Any, Iterable, Iterator, Optional

import fsspec
import pytest
import xarray

from eopf.exceptions import GroupExistError, InvalidProductError
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
        raise IndexError()

    def __len__(self) -> int:
        return 0

    def __setitem__(self, k: Any, v: Any) -> None:
        pass

    def __delitem__(self, v: Any) -> None:
        raise IndexError()

    @property
    def map(self) -> fsspec.FSMap:
        raise IndexError()

    def listdir(self, path: Optional[str] = None) -> Iterable[str]:
        return iter([])

    def rmdir(self, path: Optional[str] = None) -> None:
        raise IndexError()

    def clear(self) -> None:
        pass

    def getsize(self, path: Optional[str] = None) -> None:
        raise NotImplementedError()

    def dir_path(self, path: Optional[str] = None) -> None:
        raise NotImplementedError()

    def is_group(self, path: str) -> bool:
        raise IndexError()

    def is_variable(self, path: str) -> bool:
        raise IndexError()

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

    product.measurements["group1"].add_group("group_2b")
    product.measurements["group1"].add_group("group_2b/group_3")
    product.add_group("measurements/group1/group_2b/group_3b")

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
    assert_contain(product, "measurements/group1/group2b/group_3b", EOGroup)
    assert_contain(product, "measurements/group1/group2b/group_3b/", EOGroup)
    assert_contain(product, "measurements/group1/variable_a", EOVariable)
    assert_contain(product, "measurements/group1/group2/variable_b", EOVariable)
    assert_contain(product, "measurements/group1/group2/variable_c", EOVariable)
    assert_contain(product, "measurements/group1/group2/variable_d", EOVariable)

    with pytest.raises(IndexError):
        assert_contain(product, "measurements/group2", EOGroup)
    with pytest.raises(IndexError):
        assert_contain(product, "measurements/group1/variable_d", EOVariable)


@pytest.mark.unit
def test_invalids_add():
    product = fill_test_product()
    product.add_variable("measurements/group1/group2/variable_c")
    product["measurements/group1/group2"].add_variable("variable_d")
    with pytest.raises(GroupExistError):
        product.add_group("measurements/group1/group_2b")
    with pytest.raises(GroupExistError):
        product.measurements["group1"].add_group("group_2b")
    with pytest.raises(InvalidProductError):
        product.add_variable("direct_var")


@pytest.mark.unit
def test_coordinates():
    product = fill_test_product()
    paths = [
        "/",
        "measurements",
        "measurements/group1",
        "measurements/group1/group2",
        "measurements/group1/group2/variable_c",
    ]
    c1 = {p: product.add_variable("coordinates/" + p + "/c1") for p in paths}

    c2 = product.add_variable("coordinates/measurements/group1/c_2")

    expected_dict = {"c_1": c1[paths[2]], "c_2": c2}
    assert product[paths[2]].coordinates == dict(expected_dict)
    for p in paths:
        assert product[p].get_coordinate("c1") == c1[p]

    for p in paths:
        if paths[2] in p:
            assert product[p].get_coordinate("c2") == c2
        else:
            with pytest.raises(IndexError):
                product[p].get_coordinate("c2")


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
