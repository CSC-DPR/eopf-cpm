import contextlib
import os
from unittest import mock

import pytest

from eopf.computing import eopf_breakpoint, eopf_class_breakpoint
from eopf.computing.breakpoint import BreakMode, _retrieve_product, _write_product
from eopf.product import EOProduct
from eopf.product.conveniences import init_product, open_store


def fake_write(*args, **kwargs):
    return "writed"


def fake_retrieve(*args, **kwargs):
    return "retrieved"


def fake_func(*args, **kwargs):
    return "produced"


class FakeClass:
    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def run(*args, **kwargs):
        return "produced"

    def other(*args, **kwargs):
        return "other"

    def _method(*args, **kwargs):
        return "value"


@pytest.fixture
def fake_retrieve_product(OUTPUT_DIR):
    fake_retrieve_product = init_product("product", storage=os.path.join(OUTPUT_DIR, "fake_retrieve_computing.zarr"))
    with open_store(fake_retrieve_product, mode="w"):
        fake_retrieve_product.write()
    return fake_retrieve_product.store.url


@pytest.fixture
def fake_url():
    return "_a_complete_false_url_.zarr"


@pytest.mark.unit
@pytest.mark.parametrize(
    "allowed_mode, breakpoint_params, expected_data, expected_exception",
    [
        (
            [BreakMode.FORCE_WRITE, BreakMode.RETRIEVE, BreakMode.SKIP],
            {"break_mode": BreakMode.RETRIEVE, "storage": "fake_storage", "store_params": {}},
            "retrieved",
            [],
        ),
        (
            [BreakMode.FORCE_WRITE, BreakMode.RETRIEVE, BreakMode.SKIP],
            {"break_mode": BreakMode.FORCE_WRITE, "storage": "fake_storage", "store_params": {}},
            "writed",
            [],
        ),
        (
            [BreakMode.FORCE_WRITE, BreakMode.RETRIEVE, BreakMode.SKIP],
            {"break_mode": BreakMode.SKIP, "storage": "fake_storage", "store_params": {}},
            "produced",
            [],
        ),
        (
            [],
            {"break_mode": BreakMode.SKIP, "storage": "fake_storage", "store_params": {}},
            "produced",
            [],
        ),
        (
            [],
            {"break_mode": BreakMode.FORCE_WRITE, "storage": "fake_storage", "store_params": {}},
            "produced",
            [],
        ),
        (
            [],
            {"break_mode": BreakMode.RETRIEVE, "storage": "fake_storage", "store_params": {}},
            "produced",
            [],
        ),
        (
            [BreakMode.FORCE_WRITE, BreakMode.RETRIEVE, BreakMode.SKIP],
            {"break_mode": BreakMode.RETRIEVE, "storage": "", "store_params": {}},
            "produced",
            [],
        ),
        (
            [],
            {"break_mode": "fake_mode", "storage": "", "store_params": {}},
            "",
            [(ValueError, "Invalid value given for break_mode: .*")],
        ),
        (
            [],
            {"break_mode": 1, "storage": "", "store_params": {}},
            "",
            [(TypeError, "Unrecognized break_mode .* should be an instance of BreakMode but is .*")],
        ),
    ],
)
def test_breakpoint_on_func(allowed_mode, breakpoint_params, expected_data, expected_exception):
    wrapper = eopf_breakpoint(fake_func, allowed_mode=allowed_mode, retrieve=fake_retrieve, write=fake_write)
    for test_mode in [True, False]:
        if test_mode:
            wrapper = eopf_breakpoint(fake_func, allowed_mode=allowed_mode, retrieve=fake_retrieve, write=fake_write)
        else:
            wrapper = eopf_breakpoint(allowed_mode=allowed_mode, retrieve=fake_retrieve, write=fake_write)(fake_func)
        with contextlib.ExitStack() as stack:
            for exception, match_reg in expected_exception:
                stack.enter_context(pytest.raises(exception, match=match_reg))
            p = wrapper(**breakpoint_params)
            assert p == expected_data

        if (
            breakpoint_params["break_mode"] == BreakMode.RETRIEVE
            and BreakMode.RETRIEVE in allowed_mode
            and breakpoint_params["storage"]
        ):
            with mock.patch("tests.computing.test_breakpoint.fake_retrieve", return_value=None):
                wrapper = eopf_breakpoint(
                    fake_func,
                    allowed_mode=allowed_mode,
                    retrieve=fake_retrieve,
                    write=fake_write,
                )
                p = wrapper(**breakpoint_params)
                assert p != expected_data


@pytest.mark.unit
@pytest.mark.parametrize(
    "wrapped_methods, expected_calls",
    [
        ([], 3),
        (["run"], 1),
        (["other"], 1),
        (["run", "other"], 2),
        (["run", "__call__"], 2),
        (["run", "_method"], 1),
    ],
)
def test_breakpoint_on_class(wrapped_methods, expected_calls):
    for test_mode in [True, False]:
        with mock.patch("eopf.computing.breakpoint.eopf_breakpoint") as f:
            if test_mode:
                WrappedClass = eopf_class_breakpoint(
                    FakeClass,
                    methods=wrapped_methods,
                    write=fake_write,
                    retrieve=fake_retrieve,
                )
            else:
                WrappedClass = eopf_class_breakpoint(methods=wrapped_methods, write=fake_write, retrieve=fake_retrieve)(
                    FakeClass,
                )
            WrappedClass()
        assert f.call_count == expected_calls


@pytest.mark.unit
def test_write_product():
    with (
        mock.patch("eopf.product.core.eo_product.EOProduct.open") as open_product,
        mock.patch("eopf.product.core.eo_product.EOProduct.write", return_value=None) as write_product,
    ):
        _write_product(EOProduct(""), "")
    assert open_product.call_count == 2
    assert all(
        ("mode", mode) in kwargs
        for mode, kwargs in zip(["w", "r"], [c.kwargs.items() for c in open_product.call_args_list])
    )
    assert write_product.call_count == 1


@pytest.mark.unit
def test_retrieve_product(fake_retrieve_product, fake_url):
    product = _retrieve_product(storage=fake_retrieve_product)
    assert isinstance(product, EOProduct)
    assert product.store.url == fake_retrieve_product

    product = _retrieve_product(fake_url)
    assert product is None
