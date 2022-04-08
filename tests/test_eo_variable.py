import operator

import numpy as np
import pytest
from hypothesis import given
from hypothesis import strategies as st
from hypothesis.extra import numpy as xps

from eopf.product.core import EOVariable

from .utils import realize_strategy


@st.composite
def eovariable_strategie(draw, name="", data=None, dims=tuple(), with_input=True):
    var_data = draw(realize_strategy(data))
    var_dims = draw(realize_strategy(dims))

    variable = EOVariable(name, data=var_data, dims=var_dims)
    if not with_input:
        return variable

    if not var_dims:
        if hasattr(var_data, "shape"):
            shape = var_data.shape
        else:
            shape = np.array(var_data).shape
        var_dims = tuple(f"dim_{i}" for i in range(len(shape)))

    return variable, var_data, var_dims


@pytest.mark.unit
@pytest.mark.parametrize(
    "data, dims",
    [
        (np.array([[1, 2], [3, 4]]), "a"),
        (np.array([[1, 2], [3, 4]]), ("a",)),
        (np.array([[1, 2], [3, 4]]), ("a", "b", "c")),
        (None, "a"),
        (None, ("a",)),
        (None, ("a", "b")),
        (None, ("a", "b", "c")),
    ],
)
def test_set_dimensions(data, dims):
    with pytest.raises(ValueError):
        EOVariable("var", data=data, dims=dims)


@pytest.mark.unit
@pytest.mark.parametrize("new_dims", [("21", "34"), (22, 33)])
@given(
    variable=st.one_of(
        eovariable_strategie(data=xps.arrays(dtype="float64", shape=(2, 2))),
        eovariable_strategie(data=xps.arrays(dtype="float64", shape=(2, 2)), dims=("a", "b")),
    ),
)
def test_multiple_dims(variable, new_dims):
    var, _, dims = variable
    assert var.dims == dims
    assert var.dims == var._data.dims
    with pytest.raises(ValueError):
        var.assign_dims(("a",))
    with pytest.raises(ValueError):
        var.assign_dims(("a", "b", "c"))
    var.assign_dims(new_dims)
    assert var.dims == var._data.dims
    assert var.dims == new_dims


@pytest.mark.unit
@given(
    variable=st.one_of(
        eovariable_strategie(data=xps.arrays(dtype="float64", shape=tuple())),
        eovariable_strategie(data=xps.arrays(dtype="float64", shape=tuple()), dims=tuple()),
    ),
)
def test_without_dims(variable):
    var_empty, _, dims = variable
    assert var_empty.dims == dims
    assert var_empty.dims == var_empty._data.dims
    with pytest.raises(ValueError):
        var_empty.assign_dims(("a",))
    var_empty.assign_dims(tuple())
    assert var_empty.dims == tuple()
    assert var_empty._data.dims == tuple()


@pytest.mark.unit
@pytest.mark.parametrize(
    "ops_name",
    [
        "__radd__",
        "__rsub__",
        "__rmul__",
        "__add__",
        "__sub__",
        "__mul__",
        "__mod__",
        "__truediv__",
        "__floordiv__",
        "__rtruediv__",
        "__rfloordiv__",
        "__le__",
        "__lt__",
        "__ge__",
        "__gt__",
        "__eq__",
        "__ne__",
        "__pow__",
        "__rpow__",
        "__rmod__",
    ],
)
@given(
    a=eovariable_strategie(data=xps.arrays(dtype="float64", shape=(3, 3, 3)), with_input=False),
    b=eovariable_strategie(data=xps.arrays(dtype="float64", shape=(3, 3, 3)), with_input=False),
)
def test_binary_ops_eovar_mixin(a, b, ops_name):
    ops_eo = getattr(a, ops_name)(b).compute()
    assert isinstance(ops_eo, EOVariable)

    ops_xr = getattr(a._data, ops_name)(b._data).compute()

    assert np.array_equal(ops_eo, ops_xr, equal_nan=True)


@pytest.mark.unit
@pytest.mark.parametrize(
    "ops_name",
    [
        "__radd__",
        "__rsub__",
        "__rmul__",
        "__add__",
        "__sub__",
        "__mul__",
        "__mod__",
        "__truediv__",
        "__floordiv__",
        "__rtruediv__",
        "__rfloordiv__",
        "__le__",
        "__lt__",
        "__ge__",
        "__gt__",
        "__eq__",
        "__ne__",
        "__pow__",
        "__rpow__",
        "__rmod__",
    ],
)
@given(
    a=eovariable_strategie(data=xps.arrays(dtype="float64", shape=(3, 3, 3)), with_input=False),
    b=eovariable_strategie(data=xps.arrays(dtype="float64", shape=(3, 3, 4)), with_input=False),
)
def test_eovar_shape_mismatch(a, b, ops_name):
    with pytest.raises(ValueError, match=r"Shape mismatch: \([0-9, ]+\) != \([0-9, ]+\)"):
        getattr(a, ops_name)(b).compute()


@pytest.mark.unit
@pytest.mark.parametrize(
    "ops_name",
    [
        "__radd__",
        "__rsub__",
        "__rmul__",
        "__add__",
        "__sub__",
        "__mul__",
        "__mod__",
        "__truediv__",
        "__floordiv__",
        "__rtruediv__",
        "__rfloordiv__",
        "__le__",
        "__lt__",
        "__ge__",
        "__gt__",
        "__eq__",
        "__ne__",
        "__pow__",
        "__rpow__",
        "__rmod__",
    ],
)
@given(
    a=eovariable_strategie(
        data=xps.arrays(
            dtype="float64",
            shape=(3, 3, 3),
            elements=st.integers(min_value=-100, max_value=100) | st.floats(min_value=-100, max_value=100),
        ),
        with_input=False,
    ),
    b=st.one_of(st.integers(min_value=-10, max_value=10)),
)
def test_binary_ops_scalar_mixin(a, b, ops_name):
    ops_eo = getattr(a, ops_name)(b).compute()
    assert isinstance(ops_eo, EOVariable)
    ops_xr = getattr(a._data, ops_name)(b).compute()
    assert np.array_equal(ops_eo, ops_xr, equal_nan=True)


@pytest.mark.unit
@pytest.mark.parametrize("ops_name", ["__and__", "__or__", "__xor__", "__rand__", "__ror__", "__rxor__"])
@given(
    a=eovariable_strategie(data=xps.arrays(dtype="bool8", shape=(3, 3, 3), elements=st.booleans()), with_input=False),
    b=eovariable_strategie(data=xps.arrays(dtype="bool8", shape=(3, 3, 3), elements=st.booleans()), with_input=False),
)
def test_boolean_ops_mixin(a, b, ops_name):
    ops_eo = getattr(a, ops_name)(b).compute()
    assert isinstance(ops_eo, EOVariable)
    ops_xr = getattr(a._data, ops_name)(b._data).compute()
    assert np.array_equal(ops_eo, ops_xr)


@pytest.mark.unit
@pytest.mark.parametrize(
    "ops_name",
    [
        "__iadd__",
        "__isub__",
        "__imul__",
        "__ipow__",
        "__itruediv__",
        "__ifloordiv__",
        "__imod__",
    ],
)
@given(
    a=eovariable_strategie(data=xps.arrays(dtype="float64", shape=(3, 3, 3)), with_input=False),
    b=eovariable_strategie(data=xps.arrays(dtype="float64", shape=(3, 3, 3)), with_input=False),
)
def test_inplace(a, b, ops_name):
    new_a = a._data.copy()
    getattr(a, ops_name)(b).compute()
    assert isinstance(a, EOVariable)
    getattr(new_a, ops_name)(b._data).compute()
    assert np.array_equal(a, new_a, equal_nan=True)
    assert new_a is not a._data


@pytest.mark.unit
@pytest.mark.parametrize(
    "ops_name",
    [
        "__iadd__",
        "__isub__",
        "__imul__",
        "__ipow__",
        "__itruediv__",
        "__ifloordiv__",
        "__imod__",
    ],
)
@given(
    a=eovariable_strategie(data=xps.arrays(dtype="float64", shape=(3, 3, 3)), with_input=False),
    b=eovariable_strategie(data=xps.arrays(dtype="float64", shape=(3, 3, 3)), with_input=False),
)
def test_ops_inplace(a, b, ops_name):
    new_a = a._data.copy()
    getattr(a, ops_name)(b).compute()

    assert isinstance(a, EOVariable)
    getattr(new_a, ops_name)(b._data).compute()
    assert np.array_equal(a, new_a, equal_nan=True)
    assert new_a is not a._data


@pytest.mark.unit
@pytest.mark.parametrize(
    "ops_name",
    [
        "__ixor__",
        "__ior__",
        "__iand__",
    ],
)
@given(
    a=eovariable_strategie(data=xps.arrays(dtype="bool8", shape=(3, 3, 3)), with_input=False),
    b=eovariable_strategie(data=xps.arrays(dtype="bool8", shape=(3, 3, 3)), with_input=False),
)
def test_bool_ops_inplace(a, b, ops_name):
    new_a = a._data.copy()
    getattr(a, ops_name)(b).compute()
    assert isinstance(a, EOVariable)
    getattr(new_a, ops_name)(b._data).compute()
    assert np.array_equal(a, new_a, equal_nan=True)
    assert new_a is not a._data


@pytest.mark.unit
@pytest.mark.parametrize(
    "func",
    [
        pytest.param(operator.neg, id="negate"),
        pytest.param(operator.pos, id="pos"),
        pytest.param(operator.abs, id="abs"),
        pytest.param(operator.invert, id="invert"),
        pytest.param(np.round, id="round"),
        pytest.param(np.conj, id="conj"),
        pytest.param(np.conjugate, id="conjugate"),
    ],
)
@given(
    a=eovariable_strategie(
        data=xps.arrays(dtype="int64", shape=(3, 3), elements={"allow_nan": False}),
        with_input=False,
    ),
)
def test_unary_ops(a, func):
    ops_eo = func(a).compute()
    assert isinstance(ops_eo, EOVariable)
    ops_xr = func(a._data).compute()
    assert np.array_equal(ops_eo, ops_xr, equal_nan=True)


@pytest.mark.unit
@pytest.mark.parametrize(
    "func",
    [
        pytest.param(np.argsort, id="argsort"),
    ],
)
@given(
    a=eovariable_strategie(
        data=xps.arrays(dtype="int64", shape=(3, 3), elements={"allow_nan": False}),
        with_input=False,
    ),
)
def test_unary_ops_failed(a, func):
    with pytest.raises(NotImplementedError, match="'argsort' is not yet a valid method on dask arrays"):
        func(a).compute()
    with pytest.raises(NotImplementedError, match="'argsort' is not yet a valid method on dask arrays"):
        func(a._data).compute()


@pytest.mark.unit
@pytest.mark.parametrize(
    "conv",
    [
        float,
        complex,
        int,
        bool,
    ],
)
@given(
    a=eovariable_strategie(
        data=xps.arrays(dtype="int64", shape=(1,), elements={"allow_nan": False}),
        with_input=False,
    ),
)
def test_conversion_1d(a, conv):
    conv_a = conv(a)
    assert isinstance(conv_a, conv)
    assert np.array_equal(conv_a, conv(a._data))


@pytest.mark.unit
@pytest.mark.parametrize(
    "conv, exc",
    [
        (float, TypeError),
        (complex, TypeError),
        (int, TypeError),
        (bool, ValueError),
    ],
)
@given(
    a=eovariable_strategie(
        data=xps.arrays(dtype="int64", shape=(3, 3), elements={"allow_nan": False}),
        with_input=False,
    ),
)
def test_conversion_failed(a, conv, exc):
    with pytest.raises(exc):
        conv(a)
    with pytest.raises(exc):
        conv(a._data)
