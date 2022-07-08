import enum
import logging
import warnings
from functools import wraps
from typing import Any, Callable, Optional, Union

import fsspec

from eopf.product import EOProduct
from eopf.product.store import EOZarrStore

__all__ = ["BreakMode", "eopf_breakpoint", "eopf_class_breakpoint"]


logger = logging.getLogger("eopf")


class BreakMode(enum.Enum):
    """Given mode for EOPF BreakPoint"""

    FORCE_WRITE = "w"
    RETRIEVE = "r"
    SKIP = "s"


def _retrieve_product(storage: str, **store_params: Any) -> Optional[EOProduct]:
    """Try to retrieve a zarr product from storage information.

    If the product can't be reach, return None.

    Parameters
    ----------
    storage: str
        uri to the zarr product
    store_params: Any
        parameters to access to the product

    Returns
    -------
    None or EOProduct
    """
    store = EOZarrStore(storage)

    proto, name = fsspec.core.split_protocol(storage)
    filesystem = fsspec.get_filesystem_class(proto)(**store_params)
    if filesystem.exists(storage):
        return EOProduct(name, store).open(mode="r", **store_params)
    return None


def _write_product(product: EOProduct, storage: str, **store_params: Any) -> EOProduct:
    """Write a product in zarr.

    Parameters
    ----------
    product: EOProduct
        product to write
    storage: str
        uri to the zarr object
    store_params: Any
        parameters to access to the zarr

    Returns
    -------
    EOProduct
    """
    store = EOZarrStore(storage)
    with product.open(mode="w", storage=store, **store_params):
        product.write()
    return EOProduct(product.name, store).open(mode="r", **store_params)


def eopf_breakpoint(
    func: Optional[Callable[..., Any]] = None,
    allowed_mode: list[BreakMode] = [BreakMode.FORCE_WRITE, BreakMode.RETRIEVE, BreakMode.SKIP],
    retrieve: Callable[..., Any] = _retrieve_product,
    write: Callable[..., Any] = _write_product,
) -> Callable[..., Any]:
    """A decorator use to wrap callable with break point mechanism

    After applying this decorator, the wrapped callable will take 3 named parameters:

    * break_mode: BreakMode
        indicate the current break mode (the data should be write or retrieve ?)
    * storage: str
        uri of the target data (where to read / write)
    * store_params: dict[str, Any]
        parameters to open the target file

    Parameters
    ----------
    func: callable, optional
        Callable we want to wrap
    allowed_mode: list
        list of BreakMode that are allowed for this breakpoint (default: all)
    retrieve: callable
        callable used to retrieve the desire object, should return same object type as func
        or None if object can't be retrieve
    write: callable
        callable used to write the desire object, should return same object type as func

    Returns
    -------
    callable

    Examples
    --------
    >>> from eopf.computing.breakpoint import eopf_breakpoint
    >>> from eopf.product import EOGroup, EOProduct
    >>> from eopf.product.conveniences import init_product
    >>> @eopf_breakpoint
    ... def my_function(groups: list[EOGroup]) -> EOProduct:
    ...    product = init_product("new_product")
    ...    for group in groups:
    ...        product[group.name] = group
    ...    return product
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def _method_wrapper(
            *args: Any,
            break_mode: Union[BreakMode, str] = BreakMode.SKIP,
            storage: str = "",
            store_params: dict[str, Any] = {},
            **kwargs: Any,
        ) -> Any:
            if not isinstance(break_mode, (BreakMode, str)):
                raise TypeError(
                    f"Unrecognized break_mode {break_mode}, "
                    f"should be an instance of BreakMode but is {type(break_mode)}",
                )
            if isinstance(break_mode, str):
                try:
                    break_mode = BreakMode(break_mode)
                except ValueError:
                    raise ValueError(f"Invalid value given for break_mode: {break_mode}")

            if break_mode not in allowed_mode:
                warnings.warn(f"BreakPoint Mode {break_mode} not allowed. BreakPoint it's skip.")
                return func(*args, **kwargs)

            if break_mode == BreakMode.SKIP or not storage:
                logger.info(f"SKIP Breakpoint for {func}")
                return func(*args, **kwargs)

            if break_mode == BreakMode.RETRIEVE:
                logger.info(f"RETRIEVE Breakpoint for {func}")
                obj = retrieve(storage, **store_params)
                if obj is not None:
                    return obj
            logger.info(f"WRITE Breakpoint for {func}")
            obj = func(*args, **kwargs)
            return write(obj, storage, **store_params)

        return _method_wrapper

    if func:
        return decorator(func)
    return decorator


def eopf_class_breakpoint(
    klass: type = None,
    allowed_mode: list[BreakMode] = [BreakMode.FORCE_WRITE, BreakMode.RETRIEVE, BreakMode.SKIP],
    methods: list[str] = [],
    retrieve: Callable[..., Any] = _retrieve_product,
    write: Callable[..., Any] = _write_product,
) -> Callable[..., Any]:
    """A decorator use to wrap methods of a class with break point mechanism

    After applying this decorator, all wrapped methods will take 3 named parameters:

    * break_mode: BreakMode
        indicate the current break mode (the data should be write or retrieve ?)
    * storage: str
        uri of the target data (where to read / write)
    * store_params: dict[str, Any]
        parameters to open the target file

    Parameters
    ----------
    klass: type, optional
        class to wraps methds
    allowed_mode: list
        list of BreakMode that are allowed for this breakpoint (default: all)
    methods: list[str]
        list of methods name to wrap, if empty, all public methods and/or ``__call__`` method are wrapped
    retrieve: callable
        callable used to retrieve the desire object, should return same object type as the wrapped method
        or None if object can't be retrieve
    write: callable
        callable used to write the desire object, should return same object type as the wrapped method

    Returns
    -------
    callable

    Examples
    --------
    >>> from eopf.computing import EOProcessingUnit
    >>> from eopf.computing.breakpoint import eopf_breakpoint
    >>> from eopf.product import EOGroup, EOProduct
    >>> from eopf.product.conveniences import init_product
    >>> @eopf_class_breakpoint
    ... class MyProcessingUnit(EOProcessingUnit):
    ...    def run(input_product: EOProduct, **kwargs: Any) -> EOProduct:
    ...        product = init_product("new_product")
    ...        for var_name, variable in input_product.measurements:
    ...            product.measurements.add_variable(var_name, variable)
    ...        return product
    """

    def decorator(klass: type) -> Callable[..., Any]:
        @wraps(klass)
        def cls_wrapper(*cls_args: Any, **cls_kwargs: Any) -> Any:
            instance = klass(*cls_args, **cls_kwargs)
            for method_name in methods or dir(instance):
                elm = getattr(instance, method_name)
                if method_name == "__call__" or (not method_name.startswith("_") and callable(elm)):
                    setattr(
                        instance,
                        method_name,
                        eopf_breakpoint(elm, allowed_mode=allowed_mode, retrieve=retrieve, write=write),
                    )
            return instance

        return cls_wrapper

    if klass:
        return decorator(klass)
    return decorator
