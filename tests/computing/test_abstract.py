import contextlib
from typing import Any
from unittest import mock

import numpy as np
import pytest
from dask import array as da
from numpy.typing import DTypeLike
from pytest_lazyfixture import lazy_fixture

from eopf.computing import EOProcessingStep, EOProcessingUnit, EOProcessor
from eopf.computing.abstract import EOBlockProcessingStep, EOOverlapProcessingStep
from eopf.product import EOProduct
from eopf.product.conveniences import init_product

from ..utils import assert_is_subeocontainer


class SumProcessStep(EOProcessingStep):
    def apply(
        self, *inputs: np.ndarray[Any, np.dtype[Any]], dtype: DTypeLike = float, **kwargs: Any
    ) -> np.ndarray[Any, np.dtype[Any]]:
        return sum(inputs)


class SumProcessingUnit(EOProcessingUnit):
    def run(self, product: EOProduct, **kwargs: Any) -> EOProduct:
        paths = kwargs.get("variables_paths", [])
        dest_path = kwargs.get("dest_path", "/variable")
        step = SumProcessStep()
        new_da = step.apply(*[product[path].data for path in paths])
        new_product = EOProduct("new_product")
        new_product.add_variable(dest_path, data=new_da)
        return new_product


class SumProcessor(EOProcessor):
    def run(self, product: EOProduct, **kwargs: Any) -> EOProduct:
        with contextlib.ExitStack() as stack:
            if product.store is not None:
                stack.enter_context(product.open(mode="r"))
            paths = kwargs.get("variables_paths", [])
            dest_path = kwargs.get("dest_path", "/variable")
            step = SumProcessStep()
            new_da = step.apply(*[product[path].data for path in paths])
            new_product = init_product("new_product")
            new_product.add_variable(dest_path, data=new_da)
        return new_product


class SumBlockProcessingStep(EOBlockProcessingStep):
    def func(self, *inputs: np.ndarray[Any, np.dtype[Any]], **kwargs: Any) -> np.ndarray[Any, np.dtype[Any]]:
        return sum(inputs)


class SumOverlapProcessingStep(EOOverlapProcessingStep):
    def func(self, *inputs: np.ndarray[Any, np.dtype[Any]], **kwargs: Any) -> np.ndarray[Any, np.dtype[Any]]:
        return sum(inputs)


@pytest.fixture
def variable_paths():
    return [
        "measurements/images/oa10_radiance",
        "measurements/images/oa4_radiance",
        "measurements/images/oa1_radiance",
        "measurements/images/oa2_reflectance",
        "condition/missing_value",
        "condition/radiometry/mm",
    ]


@pytest.fixture
def fake_product(variable_paths):
    product = EOProduct("fake_product")
    for path in variable_paths:
        product.add_variable(path, data=np.array([15]))
    return product


@pytest.fixture
def output_expected_product(variable_paths):
    product = EOProduct("expected_product")
    product.add_variable("/measurements/variable", np.array([15]) * len(variable_paths))
    return product


@pytest.fixture
def valide_output_expected_product(variable_paths):
    product = init_product("expected_product")
    product.add_variable("/measurements/variable", np.array([15]) * len(variable_paths))
    return product


@pytest.mark.unit
@pytest.mark.parametrize(
    "dasks_arrays, processing_step, kwargs, expected_id",
    [
        (
            [da.asarray(np.array([1])) for _ in range(10)],
            SumProcessStep("identifier"),
            {},
            "identifier",
        ),
        (
            [da.asarray(np.array([1])) for _ in range(10)],
            SumBlockProcessingStep("identifier"),
            {},
            "identifier",
        ),
        (
            [da.asarray(np.array([1])) for _ in range(10)],
            SumOverlapProcessingStep("identifier"),
            {},
            "identifier",
        ),
    ],
)
def test_abstract_processing_step(
    dasks_arrays: da.Array,
    processing_step: EOProcessingStep,
    kwargs,
    expected_id,
):
    assert processing_step.identifier == expected_id
    with mock.patch(f"{processing_step.__class__.__module__}.{processing_step.__class__.__name__}.apply") as mock_apply:
        processing_step.apply(*dasks_arrays, **kwargs)
    mock_apply.assert_called_once_with(*dasks_arrays, **kwargs)
    assert all(isinstance(i, da.Array) for i in mock_apply.call_args.args)
    assert kwargs == mock_apply.call_args.kwargs


@pytest.mark.unit
@pytest.mark.parametrize(
    "dasks_arrays, processing_step, kwargs, expected_id",
    [
        (
            [da.asarray(np.array([1])) for _ in range(10)],
            SumBlockProcessingStep("identifier"),
            {},
            "identifier",
        ),
        (
            [da.asarray(np.array([1])) for _ in range(10)],
            SumOverlapProcessingStep("identifier"),
            {},
            "identifier",
        ),
    ],
)
def test_maps_processing_step(
    dasks_arrays: da.Array,
    processing_step: EOProcessingStep,
    kwargs,
    expected_id,
):
    assert processing_step.identifier == expected_id
    with mock.patch(f"{processing_step.__class__.__module__}.{processing_step.__class__.__name__}.func") as mock_func:
        mock_func.side_effect = lambda *x: sum(x)
        ret_val = processing_step.apply(*dasks_arrays, **kwargs).compute()
    assert isinstance(ret_val, np.ndarray)
    assert all(isinstance(i, np.ndarray) for i in mock_func.call_args.args)
    assert kwargs == mock_func.call_args.kwargs


@pytest.mark.unit
@pytest.mark.parametrize(
    "product, kwargs, processing_unit, expected_id",
    [
        (
            lazy_fixture("fake_product"),
            {
                "variables_paths": [
                    "measurements/images/oa10_radiance",
                    "measurements/images/oa4_radiance",
                    "measurements/images/oa1_radiance",
                    "measurements/images/oa2_reflectance",
                    "condition/missing_value",
                    "condition/radiometry/mm",
                ],
                "dest_path": "/measurements/variable",
            },
            SumProcessingUnit("identifier"),
            "identifier",
        ),
    ],
)
def test_abstract_processing_unit(product, kwargs, processing_unit, expected_id):
    assert str(processing_unit) == f"{processing_unit.__class__.__name__}<{processing_unit.identifier}>"
    assert (
        repr(processing_unit)
        == f"[{id(processing_unit)}]{processing_unit.__class__.__name__}<{processing_unit.identifier}>"
    )
    assert processing_unit.identifier == expected_id
    with mock.patch(f"{processing_unit.__class__.__module__}.{processing_unit.__class__.__name__}.run") as mock_run:
        processing_unit.run(product, **kwargs)
    mock_run.assert_called_once_with(product, **kwargs)
    assert all(isinstance(p, EOProduct) for p in mock_run.call_args.args)
    assert kwargs == mock_run.call_args.kwargs


@pytest.mark.unit
@pytest.mark.parametrize(
    "product, kwargs, processing_unit, expected_product, expected_id",
    [
        (
            lazy_fixture("fake_product"),
            {
                "variables_paths": [
                    "measurements/images/oa10_radiance",
                    "measurements/images/oa4_radiance",
                    "measurements/images/oa1_radiance",
                    "measurements/images/oa2_reflectance",
                    "condition/missing_value",
                    "condition/radiometry/mm",
                ],
                "dest_path": "/measurements/variable",
            },
            SumProcessor("identifier"),
            lazy_fixture("valide_output_expected_product"),
            "identifier",
        ),
    ],
)
def test_abstract_processor(product, kwargs, processing_unit, expected_product, expected_id):
    assert str(processing_unit) == f"{processing_unit.__class__.__name__}<{processing_unit.identifier}>"
    assert (
        repr(processing_unit)
        == f"[{id(processing_unit)}]{processing_unit.__class__.__name__}<{processing_unit.identifier}>"
    )

    assert processing_unit.identifier == expected_id
    with (
        mock.patch(
            f"{processing_unit.__class__.__module__}.{processing_unit.__class__.__name__}.run",
            return_value=expected_product,
        ) as mock_run,
    ):
        p = processing_unit.run_validating(product, **kwargs)
    mock_run.assert_called_once_with(product, **kwargs)
    assert all(isinstance(p, EOProduct) for p in mock_run.call_args.args)
    assert kwargs == mock_run.call_args.kwargs

    assert_is_subeocontainer(p, expected_product)
    assert_is_subeocontainer(expected_product, p)
