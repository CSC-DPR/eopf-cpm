import numpy as np
import pytest

from eopf.product.core import EOVariable


@pytest.mark.unit
def test_redimension():
    var_dim2 = []
    var_dim2.append(EOVariable("var", data=np.array([[1, 2], [3, 4]])))
    var_dim2.append(EOVariable("var", data=np.array([[1, 2], [3, 4]]), dims=("a", "b")))
    with pytest.raises(ValueError):
        var = EOVariable("var", data=np.array([[1, 2], [3, 4]]), dims=("a"))
    with pytest.raises(ValueError):
        var = EOVariable("var", data=np.array([[1, 2], [3, 4]]), dims=("a", "b", "c"))
    with pytest.raises(ValueError):
        var = EOVariable("var", dims=("a", "b"))
    var_empty = EOVariable("var", dims=())

    assert var_dim2[0].dims == ("dim_0", "dim_1")
    assert var_dim2[1].dims == ("a", "b")
    for var in var_dim2:
        assert var.dims == var._data.dims
        with pytest.raises(ValueError):
            var.assign_dims(("a",))
        with pytest.raises(ValueError):
            var.assign_dims(("a", "b", "c"))
        var.assign_dims(("21", "34"))
        assert var.dims == var._data.dims
        assert var.dims == ("21", "34")

        var.assign_dims((22, 33))
        assert var.dims == var._data.dims
        assert var.dims == (22, 33)

    assert var_empty.dims == tuple()
    assert var_empty._data.dims == tuple()
    with pytest.raises(ValueError):
        var_empty.assign_dims(("a",))
    var_empty.assign_dims(tuple())
    assert var_empty.dims == tuple()
    assert var_empty._data.dims == tuple()
