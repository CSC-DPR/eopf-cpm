import operator

import xarray as xr


class EOVariableOperatorsMixin:
    __slots__ = ()

    def __apply_binary_ops__(self, other, ops, reflexive=False):
        if isinstance(other, EOVariableOperatorsMixin):
            other_value = other._data
        else:
            other_value = other
        return type(self)(ops(self._data, other_value) if not reflexive else ops(other_value, self._data))

    def __add__(self, other):
        return self.__apply_binary_ops__(other, operator.add)

    def __sub__(self, other):
        return self.__apply_binary_ops__(other, operator.sub)

    def __mul__(self, other):
        return self.__apply_binary_ops__(other, operator.mul)

    def __pow__(self, other):
        return self.__apply_binary_ops__(other, operator.pow)

    def __truediv__(self, other):
        return self.__apply_binary_ops__(other, operator.truediv)

    def __floordiv__(self, other):
        return self.__apply_binary_ops__(other, operator.floordiv)

    def __mod__(self, other):
        return self.__apply_binary_ops__(other, operator.mod)

    def __and__(self, other):
        return self.__apply_binary_ops__(other, operator.and_)

    def __xor__(self, other):
        return self.__apply_binary_ops__(other, operator.xor)

    def __or__(self, other):
        return self.__apply_binary_ops__(other, operator.or_)

    def __lt__(self, other):
        return self.__apply_binary_ops__(other, operator.lt)

    def __le__(self, other):
        return self.__apply_binary_ops__(other, operator.le)

    def __gt__(self, other):
        return self.__apply_binary_ops__(other, operator.gt)

    def __ge__(self, other):
        return self.__apply_binary_ops__(other, operator.ge)

    def __eq__(self, other):
        return self.__apply_binary_ops__(other, operator.eq)

    def __ne__(self, other):
        return self.__apply_binary_ops__(other, operator.ne)

    def __radd__(self, other):
        return self.__apply_binary_ops__(other, operator.add, reflexive=True)

    def __rsub__(self, other):
        return self.__apply_binary_ops__(other, operator.sub, reflexive=True)

    def __rmul__(self, other):
        return self.__apply_binary_ops__(other, operator.mul, reflexive=True)

    def __rpow__(self, other):
        return self.__apply_binary_ops__(other, operator.pow, reflexive=True)

    def __rtruediv__(self, other):
        return self.__apply_binary_ops__(other, operator.truediv, reflexive=True)

    def __rfloordiv__(self, other):
        return self.__apply_binary_ops__(other, operator.floordiv, reflexive=True)

    def __rmod__(self, other):
        return self.__apply_binary_ops__(other, operator.mod, reflexive=True)

    def __rand__(self, other):
        return self.__apply_binary_ops__(other, operator.and_, reflexive=True)

    def __rxor__(self, other):
        return self.__apply_binary_ops__(other, operator.xor, reflexive=True)

    def __ror__(self, other):
        return self.__apply_binary_ops__(other, operator.or_, reflexive=True)

    def __apply_inplace_ops__(self, other, ops):
        if isinstance(other, EOVariableOperatorsMixin):
            other_value = other._data
        else:
            other_value = other
        self._data = ops(self._data, other_value)
        return self

    def __iadd__(self, other):
        return self.__apply_inplace_ops__(other, operator.iadd)

    def __isub__(self, other):
        return self.__apply_inplace_ops__(other, operator.isub)

    def __imul__(self, other):
        return self.__apply_inplace_ops__(other, operator.imul)

    def __ipow__(self, other):
        return self.__apply_inplace_ops__(other, operator.ipow)

    def __itruediv__(self, other):
        return self.__apply_inplace_ops__(other, operator.itruediv)

    def __ifloordiv__(self, other):
        return self.__apply_inplace_ops__(other, operator.ifloordiv)

    def __imod__(self, other):
        return self.__apply_inplace_ops__(other, operator.imod)

    def __iand__(self, other):
        return self.__apply_inplace_ops__(other, operator.iand)

    def __ixor__(self, other):
        return self.__apply_inplace_ops__(other, operator.ixor)

    def __ior__(self, other):
        return self.__apply_inplace_ops__(other, operator.ior)

    def __apply_unary_ops__(self, ops, *args, **kwargs):
        return type(self)(ops(self._data), *args, **kwargs)

    def __neg__(self):
        return self.__apply_unary_ops__(operator.neg)

    def __pos__(self):
        return self.__apply_unary_ops__(operator.pos)

    def __abs__(self):
        return self.__apply_unary_ops__(operator.abs)

    def __invert__(self):
        return self.__apply_unary_ops__(operator.invert)

    def round(self, *args, **kwargs):
        return self.__apply_unary_ops__(self._data.round_, *args, **kwargs)

    def argsort(self, *args, **kwargs):
        return self.__apply_unary_ops__(self._data.argsort, *args, **kwargs)

    def conj(self, *args, **kwargs):
        return self.__apply_unary_ops__(self._data.conj, *args, **kwargs)

    def conjugate(self, *args, **kwargs):
        return self.__apply_unary_ops__(self._data.conjugate, *args, **kwargs)

    __add__.__doc__ = operator.add.__doc__
    __sub__.__doc__ = operator.sub.__doc__
    __mul__.__doc__ = operator.mul.__doc__
    __pow__.__doc__ = operator.pow.__doc__
    __truediv__.__doc__ = operator.truediv.__doc__
    __floordiv__.__doc__ = operator.floordiv.__doc__
    __mod__.__doc__ = operator.mod.__doc__
    __and__.__doc__ = operator.and_.__doc__
    __xor__.__doc__ = operator.xor.__doc__
    __or__.__doc__ = operator.or_.__doc__
    __lt__.__doc__ = operator.lt.__doc__
    __le__.__doc__ = operator.le.__doc__
    __gt__.__doc__ = operator.gt.__doc__
    __ge__.__doc__ = operator.ge.__doc__
    __eq__.__doc__ = xr.DataArray.__eq__.__doc__
    __ne__.__doc__ = xr.DataArray.__ne__.__doc__
    __radd__.__doc__ = operator.add.__doc__
    __rsub__.__doc__ = operator.sub.__doc__
    __rmul__.__doc__ = operator.mul.__doc__
    __rpow__.__doc__ = operator.pow.__doc__
    __rtruediv__.__doc__ = operator.truediv.__doc__
    __rfloordiv__.__doc__ = operator.floordiv.__doc__
    __rmod__.__doc__ = operator.mod.__doc__
    __rand__.__doc__ = operator.and_.__doc__
    __rxor__.__doc__ = operator.xor.__doc__
    __ror__.__doc__ = operator.or_.__doc__
    __iadd__.__doc__ = operator.iadd.__doc__
    __isub__.__doc__ = operator.isub.__doc__
    __imul__.__doc__ = operator.imul.__doc__
    __ipow__.__doc__ = operator.ipow.__doc__
    __itruediv__.__doc__ = operator.itruediv.__doc__
    __ifloordiv__.__doc__ = operator.ifloordiv.__doc__
    __imod__.__doc__ = operator.imod.__doc__
    __iand__.__doc__ = operator.iand.__doc__
    __ixor__.__doc__ = operator.ixor.__doc__
    __ior__.__doc__ = operator.ior.__doc__
    __neg__.__doc__ = operator.neg.__doc__
    __pos__.__doc__ = operator.pos.__doc__
    __abs__.__doc__ = operator.abs.__doc__
    __invert__.__doc__ = operator.invert.__doc__
    round.__doc__ = xr.DataArray.round.__doc__
    argsort.__doc__ = xr.DataArray.argsort.__doc__
    conj.__doc__ = xr.DataArray.conj.__doc__
    conjugate.__doc__ = xr.DataArray.conjugate.__doc__
