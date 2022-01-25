from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from pyfakefs.fake_filesystem import FakeFilesystem

from eopf.product.store import EOProductStore, EOZarrStore


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(_type=st.sampled_from([EOZarrStore]))
def test_write_stores(fs: FakeFilesystem, _type: EOProductStore):
    store = _type("file://product_name")

    store.open(mode="w")
    store.add_group("a_group")
    store.close()

    assert fs.isfile("product_name") or fs.isdir("product_name/a_group") or fs.isfile("product_name/a_group")


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(_type=st.sampled_from([EOZarrStore]))
def test_read_stores(fs: FakeFilesystem, _type: EOProductStore):
    store = _type("product_name")

    store.open(mode="w")
    store.add_group("a_group")
    store.close()

    store.open(mode="r")
    assert store["a_group"] is not None
    store.close()
