from abc import ABC, abstractmethod
from typing import Any

import numpy as np
from dask import array as da
from numpy.typing import DTypeLike

from eopf.product import EOProduct


class EOProcessingStep(ABC):
    """Abstract Base class For Processing Step.

    EOProcessingStep are available to produce algorithm working with
    dask.array.Array and produce an dask.array.Array.

    Parameters
    ----------
    identifier: str, optional
        a string to identify this processing step (usefull for logging)

    Attributes
    ----------
    identifier: str

    See Also
    --------
    dask.array.Array
    """

    _identifier: Any

    @property
    def identifier(self) -> Any:
        """Identifier of the processing step"""
        return self._identifier

    def __init__(self, identifier: Any = ""):
        self._identifier = identifier or str(id(self))

    @abstractmethod
    def apply(self, *args: da.Array, dtype: DTypeLike = float, **kwargs: Any) -> da.Array:  # pragma: no cover
        """Abstract method implemented in subclass to provide the algorithme

        Parameters
        ----------
        *args: dask.array.Array
            inputs dask array to compute for this algorithm
        dtype: DTypeLike
            dtype for the output dask array
        **kwargs: any
            any needed kwargs

        Returns
        -------
        dask.array.Array
        """


class EOBlockProcessingStep(EOProcessingStep):
    """Abstract class to applicate low level algorithm to chunk block of da.Array

    Parameters
    ----------
    identifier: str, optional
        a string to identify this processing step (usefull for logging and tracing)

    Attributes
    ----------
    identifier: str

    See Also
    --------
    dask.array.map_blocks
    """

    def apply(self, *args: da.Array, dtype: DTypeLike = float, **kwargs: Any) -> da.Array:
        """apply map_blocks on all given dask array

        Parameters
        ----------
        *args: dask.array.Array
            inputs dask array to compute for this algorithm
        dtype: DTypeLike
            dtype for the output dask array
        **kwargs: any
            any needed kwargs

        Returns
        -------
        dask.array.Array
        """
        return da.map_blocks(self.func, *args, dtype=dtype, **kwargs)

    @abstractmethod
    def func(
        self, *args: np.ndarray[Any, np.dtype[Any]], **kwargs: Any
    ) -> np.ndarray[Any, np.dtype[Any]]:  # pragma: no cover
        """Abstract method of the low level interface for the algorithm

        Parameters
        ----------
        *args: numpy.ndarray
            inputs numpy array from the map_blocks
        **kwargs: any
            any needed kwargs

        Returns
        -------
        numpy.ndarray
        """


class EOOverlapProcessingStep(EOProcessingStep):
    """Abstract class to applicate low level algorithm to chunk block of da.Array

    Parameters
    ----------
    identifier: str, optional
        a string to identify this processing step (usefull for logging and tracing)

    Attributes
    ----------
    identifier: str

    See Also
    --------
    dask.array.map_overlap
    """

    def apply(self, *args: da.Array, dtype: DTypeLike = float, depth: int = 1, **kwargs: Any) -> da.Array:
        return da.map_overlap(self.func, *args, depth=depth, dtype=dtype, meta=np.array((), dtype=dtype), **kwargs)

    @abstractmethod
    def func(
        self, *args: np.ndarray[Any, np.dtype[Any]], **kwargs: Any
    ) -> np.ndarray[Any, np.dtype[Any]]:  # pragma: no cover
        """Abstract method of the low level interface for the algorithm

        Parameters
        ----------
        *args: numpy.ndarray
            inputs numpy array from the map_overlap
        **kwargs: any
            any needed kwargs

        Returns
        -------
        numpy.ndarray
        """


class EOProcessingUnit(ABC):
    """Abstract class to applicate high level algorithm direclty on EOProducts

    Parameters
    ----------
    identifier: str, optional
        a string to identify this processing unit (usefull for logging and tracing)

    Attributes
    ----------
    identifier: str

    See Also
    --------
    eopf.product.EOProduct
    """

    _identifier: Any

    @property
    def identifier(self) -> Any:
        """Identifier of the processing step"""
        return self._identifier

    def __init__(self, identifier: Any = "") -> None:
        self._identifier = identifier or str(id(self))

    @abstractmethod
    def run(self, *args: EOProduct, **kwargs: Any) -> EOProduct:  # pragma: no cover
        """Abstract method to provide an interface for algorithm implementation

        Parameters
        ----------
        *args: EOProduct
            all the product to process in this processing unit
        **kwargs: any
            any needed kwargs
        """

    def __str__(self) -> str:
        return f"{self.__class__.__name__}<{self.identifier}>"

    def __repr__(self) -> str:
        return f"[{id(self)}]{str(self)}"


class EOProcessor(EOProcessingUnit):
    """Abstract class to applicate high level algorithm direclty on EOProducts

    Used to provide complete and valide product

    Parameters
    ----------
    identifier: str, optional
        a string to identify this processing unit (usefull for logging and tracing)

    Attributes
    ----------
    identifier: str

    See Also
    --------
    eopf.product.EOProduct
    """

    def validate_product(self, product: EOProduct) -> None:
        """verify that the given product is valid.

        If the product is invalid, raise an exception

        See Also
        --------
        eopf.product.EOProduct.validate
        """
        product.validate()
