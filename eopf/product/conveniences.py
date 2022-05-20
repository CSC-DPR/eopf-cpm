import contextlib
from typing import Any, Iterator, Optional, Union

from eopf.exceptions import StoreNotDefinedError
from eopf.product.core import EOProduct
from eopf.product.store.abstract import EOProductStore


def init_product(
    product_name: str, *, store_or_path_url: Optional[Union[str, EOProductStore]] = None, **kwargs: Any
) -> EOProduct:
    """Convenience function to create a valid EOProduct base.

    Parameters
    ----------
    product_name: str
        name of the product to create
    store_or_path_url: Union[str, EOProductStore], optional
        a EOProductStore or a string to create to a EOZarrStore
    **kwargs: any
        Any valid named arguments for EOProduct

    Returns
    -------
    EOProduct
        newly created product

    See Also
    --------
    eopf.product.EOProduct
    eopf.product.EOProduct.is_valid
    """
    product = EOProduct(product_name, storage=store_or_path_url, **kwargs)

    for group_name in product.MANDATORY_FIELD:
        product.add_group(group_name)
    return product


@contextlib.contextmanager
def open_store(
    store_or_product: Union[EOProduct, EOProductStore], mode: str = "r", **kwargs: Any
) -> Iterator[EOProductStore]:
    """Open an EOProductStore in the given mode.

    help you to open EOProductStore from EOProduct or directly to use
    it as a standard python open function.

    Parameters
    ----------
    store_or_product: EOProductStore or EOProduct
        store to open
    mode: str, optional
        mode to open the store (default = 'r')
    kwargs: any
        store specific kwargs

    Returns
    -------
    store
        store opened with given arguments

    See Also
    --------
    EOProductStore.open
    """
    if isinstance(store_or_product, EOProduct):
        store = store_or_product.store
    else:
        store = store_or_product
    if store is None:
        raise StoreNotDefinedError()

    try:
        store.open(mode=mode, **kwargs)
        yield store
    finally:
        store.close()
