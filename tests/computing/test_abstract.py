import contextlib
from typing import Any

import numpy as np
import pytest
from dask import array as da
from numpy.typing import DTypeLike
from pytest_lazyfixture import lazy_fixture

from eopf.computing import EOProcessingStep, EOProcessingUnit, EOProcessor
from eopf.computing.abstract import EOBlockProcessingStep, EOOverlapProcessingStep
from eopf.product import EOProduct, EOVariable
from eopf.product.conveniences import init_product

from ..utils import assert_is_subeocontainer


class SumProcessStep(EOProcessingStep):
    def apply(self, *args: da.Array, dtype: DTypeLike = float, **kwargs: Any) -> EOVariable:
        arg = args[0]
        for a in args[1:]:
            arg += a
        return arg


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
    def func(self, *args: np.ndarray[Any, np.dtype[Any]], **kwargs: Any) -> np.ndarray[Any, np.dtype[Any]]:
        return sum(args)


class SumOverlapProcessingStep(EOOverlapProcessingStep):
    def func(self, *args: np.ndarray[Any, np.dtype[Any]], **kwargs: Any) -> np.ndarray[Any, np.dtype[Any]]:
        return sum(args)


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
    "dasks_arrays, processing_step, kwargs, expected_data, expected_id",
    [
        (
            [da.asarray(np.array([1])) for _ in range(10)],
            SumProcessStep("identifier"),
            {},
            np.array([10]),
            "identifier",
        ),
        (
            [da.asarray(np.array([1])) for _ in range(10)],
            SumBlockProcessingStep("identifier"),
            {},
            np.array([10]),
            "identifier",
        ),
        (
            [da.asarray(np.array([1])) for _ in range(10)],
            SumOverlapProcessingStep("identifier"),
            {},
            np.array([10]),
            "identifier",
        ),
    ],
)
def test_processing_step(dasks_arrays: da.Array, processing_step: EOProcessingStep, kwargs, expected_data, expected_id):
    assert processing_step.identifier == expected_id
    new_da = processing_step.apply(*dasks_arrays, **kwargs)
    assert new_da.compute() == expected_data


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
            SumProcessingUnit("identifier"),
            lazy_fixture("output_expected_product"),
            "identifier",
        ),
    ],
)
def test_processing_unit(product, kwargs, processing_unit, expected_product, expected_id):
    assert str(processing_unit) == f"{processing_unit.__class__.__name__}<{processing_unit.identifier}>"
    assert (
        repr(processing_unit)
        == f"[{id(processing_unit)}]{processing_unit.__class__.__name__}<{processing_unit.identifier}>"
    )

    assert processing_unit.identifier == expected_id
    new_product = processing_unit.run(product, **kwargs)
    assert_is_subeocontainer(new_product, expected_product)
    assert_is_subeocontainer(expected_product, new_product)


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
def test_processor(product, kwargs, processing_unit, expected_product, expected_id):
    assert str(processing_unit) == f"{processing_unit.__class__.__name__}<{processing_unit.identifier}>"
    assert (
        repr(processing_unit)
        == f"[{id(processing_unit)}]{processing_unit.__class__.__name__}<{processing_unit.identifier}>"
    )

    assert processing_unit.identifier == expected_id
    new_product = processing_unit.run(product, **kwargs)
    new_product.tree()
    expected_product.tree()
    assert_is_subeocontainer(new_product, expected_product)
    assert_is_subeocontainer(expected_product, new_product)
    processing_unit.validate_product(new_product)
