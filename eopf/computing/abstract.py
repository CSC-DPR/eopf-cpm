from abc import ABC, abstractmethod
from typing import Any

import numpy as np
from dask import array as da
from numpy.typing import DTypeLike

from eopf.product import EOProduct


class EOProcessingStep(ABC):
    """Converts one or several input dask arrays (of one or several variables)
    into one dask array (of one intermediate or output variable). Usually,
    application happens delayed, data is computed only when written or used.

    Derived abstract classes come with specific implementations of apply
    that e.g. call da.map_blocks() on the arrays with a function provided
    in the concrete implementation of ProcessingStep. See BlockProcessingStep.

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
    def apply(self, *inputs: da.Array, dtype: DTypeLike = float, **kwargs: Any) -> da.Array:  # pragma: no cover
        """Abstract method that creates a new dask array from dask arrays.

        Parameters
        ----------
        *inputs: dask.array.Array
            inputs dask array to use for this algorithm
        dtype: DTypeLike
             output array data type
        **kwargs: any
            any needed kwargs

        Returns
        -------
        dask.array.Array
        """


class EOBlockProcessingStep(EOProcessingStep):
    """Block-wise converts one or several input dask arrays (of one or several variables)
    into one dask array (of one intermediate or output variable). Usually,
    application happens delayed, data is computed only when written or used.

    This abstract class comes with an implementation of apply that calls map_blocks
    and an abstract method func that must be overwritten by concrete implementation classes.
    func is called for each block (=chunk) of the inputs independently and maybe concurrently.

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

    def apply(self, *inputs: da.Array, dtype: DTypeLike = float, **kwargs: Any) -> da.Array:
        """Block-wise application of a function to create a new dask array from dask arrays

        Parameters
        ----------
        *inputs: dask.array.Array
             input arrays with same number of chunks each
        dtype: DTypeLike
            output array data type
        **kwargs: any
            any needed kwargs

        Returns
        -------
        dask.array.Array
        """
        return da.map_blocks(self.func, *inputs, dtype=dtype, meta=np.array((), dtype=dtype), **kwargs)

    @abstractmethod
    def func(
        self, *inputs: np.ndarray[Any, np.dtype[Any]], **kwargs: Any
    ) -> np.ndarray[Any, np.dtype[Any]]:  # pragma: no cover
        """Abstract method that is applied for one block of the inputs.

        It creates a new numpy array from numpy arrays.

        Parameters
        ----------
        *inputs: numpy.ndarray
            input arrays with same number of chunks each
        **kwargs: any
            any needed kwargs

        Returns
        -------
        numpy.ndarray
        """


class EOOverlapProcessingStep(EOProcessingStep):
    """Block-wise converts one or several input dask arrays (of one or several variables)
    into one dask array (of one intermediate or output variable), providing spatial
    overlap between input blocks. Usually, application happens delayed, data is computed
    only when written or used.

    This abstract class comes with an implementation of apply that calls map_blocks
    and an abstract method func that must be overwritten by concrete implementation classes.
    func is called for each block (=chunk) of the inputs independently and maybe concurrently.

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

    def apply(self, *inputs: da.Array, dtype: DTypeLike = float, depth: int = 1, **kwargs: Any) -> da.Array:
        """Block-wise application of a function with some overlap buffer from adjacent blocks.
        Creates a new dask array from dask arrays.
        (Note that using trim=False is broken in dask.)

        Parameters
        ----------
        *inputs: dask.array.Array
            input arrays with same number of chunks each
        dtype: DTypeLike
            output array data type
        depth: int
            size of buffer around each block from adjacent blocks that is provided to func as part of its inputs
        **kwargs: any
            any needed kwargs

        Returns
        -------
        dask.array.Array
        """
        return da.map_overlap(self.func, *inputs, depth=depth, dtype=dtype, meta=np.array((), dtype=dtype), **kwargs)

    @abstractmethod
    def func(
        self, *inputs: np.ndarray[Any, np.dtype[Any]], **kwargs: Any
    ) -> np.ndarray[Any, np.dtype[Any]]:  # pragma: no cover
        """Abstract method that is applied for one block of the inputs extended by a buffer with data
        from adjacent blocks. It creates a new numpy array from numpy arrays.

        The output numpy array will be trimmed by the buffer size (depth parameter of apply) by dask
        before creating the dask array mosaic.

        Parameters
        ----------
        *inputs: numpy.ndarray
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
    def run(self, *inputs: EOProduct, **kwargs: Any) -> EOProduct:  # pragma: no cover
        """Abstract method to provide an interface for algorithm implementation

        Parameters
        ----------
        *inputs: EOProduct
            all the product to process in this processing unit
        **kwargs: any
            any needed kwargs
        """

    def __str__(self) -> str:
        return f"{self.__class__.__name__}<{self.identifier}>"

    def __repr__(self) -> str:
        return f"[{id(self)}]{str(self)}"


class EOProcessor(EOProcessingUnit):
    """Abstract base class of processors i.e. processing units
    that provide valid EOProducts with coordinates etc.

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

    def run_validating(self, *inputs: EOProduct, **kwargs: Any) -> EOProduct:
        """Transforms input products into a new valid EOProduct with new variables.

        Parameters
        ----------
        *inputs: EOProduct
            inputs products to combine
        **kwargs: any
            any needed kwargs

        Returns
        -------
        EOProduct
        """
        result_product = self.run(*inputs, **kwargs)
        self.validate_product(result_product)
        return result_product

    def validate_product(self, product: EOProduct) -> None:
        """verify that the given product is valid.

        If the product is invalid, raise an exception

        See Also
        --------
        eopf.product.EOProduct.validate
        """
        product.validate()
