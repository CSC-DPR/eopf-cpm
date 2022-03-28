import json
import os

import pytest

from eopf.product.store.mapping_factory import EOMappingFactory
from eopf.product.store.netcdf import EONetCDFStore
from eopf.product.store.store_factory import EOStoreFactory


@pytest.mark.unit
@pytest.mark.parametrize("filename, format, expected_type", [("netcdf.nc", "netcdf", EONetCDFStore)])
def test_store_factory(filename, format, expected_type):
    factory = EOStoreFactory()
    store = factory.get_store(filename, format)
    assert isinstance(store, expected_type)

    store = factory.get_store(filename)
    assert isinstance(store, EONetCDFStore)

    with pytest.raises(KeyError):
        store = factory.get_store(filename, "false_format")

    with pytest.raises(KeyError):
        store = factory.get_store("file.false")


@pytest.mark.unit
@pytest.mark.parametrize(
    "filename, mapping_path",
    [
        (
            "S3A_OL_1_EFR_____LN1_O_NT_002.SEN3",
            f"{os.path.dirname(__file__)}/../eopf/product/store/mapping/S3_OL_1_EFR_mapping.json",
        ),
    ],
)
def test_mapping_factory(filename, mapping_path):
    factory = EOMappingFactory()
    mapping = factory.get_mapping(filename)
    with open(mapping_path) as f:
        assert mapping == json.load(f)

    assert factory.guess_can_read({}, "") is False

    with pytest.raises(KeyError):
        factory.get_mapping("false_false")
