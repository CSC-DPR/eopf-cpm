import pytest
import xarray
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from pyfakefs.fake_filesystem import FakeFilesystem

from eopf.exceptions import StoreNotOpenError
from eopf.product.store import EOProductStore, EOZarrStore


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(_type=st.sampled_from([EOZarrStore]))
def test_write_stores(fs: FakeFilesystem, _type: type[EOProductStore]):
    store = _type("product_name")

    store.open(mode="w")
    store.add_group("a_group")
    store.close()

    assert fs.isfile("product_name") or fs.isdir("product_name/a_group") or fs.isfile("product_name/a_group")
    store.open(mode="r")
    assert "a_group" in store.listdir()
    assert "/product_name" == store.dir_path()
    store.close()


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(_type=st.sampled_from([EOZarrStore]))
def test_read_stores(fs: FakeFilesystem, _type: type[EOProductStore]):
    store = _type("a_product")

    store.open(mode="w")
    store.add_group("a_group")
    store.close()

    store.open(mode="r")
    assert store["a_group"] is not None
    assert store.map is None
    store.close()


def test_abstract_store_cant_be_instantiate():
    with pytest.raises(TypeError):
        EOProductStore("not_instantiable")


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(_type=st.sampled_from([EOZarrStore]))
def test_store_must_be_open(fs: FakeFilesystem, _type: type[EOProductStore]):
    store = _type("a_product")

    with pytest.raises(StoreNotOpenError):
        store.add_group("a_group")

    with pytest.raises(StoreNotOpenError):
        store.add_variables("a_group", xarray.Dataset())

    with pytest.raises(StoreNotOpenError):
        store.is_group("a_group")

    with pytest.raises(StoreNotOpenError):
        store.is_variable("a_group")

    with pytest.raises(StoreNotOpenError):
        len(store)

    with pytest.raises(StoreNotOpenError):
        store.iter("a_group")

    for method in ["listdir", "rmdir", "clear", "getsize", "dir_path"]:
        with pytest.raises(StoreNotOpenError):
            getattr(store, method)()


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(_type=st.sampled_from([EOZarrStore]))
def test_store_structure(fs: FakeFilesystem, _type: type[EOProductStore]):
    store = _type("a_product")
    store.open(mode="w")
    store.add_group("a_group")
    store.add_group("another_one")
    store.add_group("a_final_one")

    assert store["a_group"] is not None

    assert isinstance(store.listdir(), list)
    assert isinstance(store.listdir("another_one"), list)

    assert isinstance(store.getsize(), int)
    assert isinstance(store.getsize("another_one"), int)

    assert isinstance(store.dir_path(), str)
    assert isinstance(store.dir_path("another_one"), str)

    assert store.is_group("another_one")
    assert not store.is_variable("another_one")

    assert store.rmdir("a_group") is None

    with pytest.raises(KeyError):
        store["a_group"]

    assert store.clear() is None

    with pytest.raises(KeyError):
        store["a_final_one"]
    with pytest.raises(KeyError):
        store["another_one"]

    store.close()
