import numpy as np
import pytest
from hypothesis import given
from hypothesis import strategies as st
from hypothesis.extra.numpy import arrays

from eopf.product.core import EOVariable


@pytest.mark.unit
@given(arrays(float, (25, 10), elements=st.floats(allow_nan=False, allow_infinity=False)))
def test_add(data):
    eovariable = EOVariable(data) + EOVariable(data)
    assert np.array_equal(eovariable._ndarray.values, (data + data))


@pytest.mark.unit
@given(arrays(float, (25, 10), elements=st.floats(allow_nan=False, allow_infinity=False)))
def test_sub(data):
    eovariable = EOVariable(data) - EOVariable(data)
    assert np.array_equal(eovariable._ndarray.values, (data - data))
