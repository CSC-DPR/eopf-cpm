import contextlib
from typing import Any, Iterator, Optional, Union

from eopf.exceptions import StoreNotDefinedError
from eopf.product.core import EOProduct
from eopf.product.core.eo_container import EOContainer
from eopf.product.core.eo_group import EOGroup
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
    product = EOProduct(product_name, store_or_path_url=store_or_path_url, **kwargs)

    for group_name in product.MANDATORY_FIELD:
        product.add_group(group_name)
    return product


def merge_product(*products: EOProduct, product_name: str = "") -> EOProduct:
    def _merge(*containers: EOContainer, output: Optional[EOProduct] = None) -> EOProduct:
        if output is None:
            output = EOProduct("")
        for container in containers:
            for item in container:
                set_ = False
                item_path = container[item].path
                for other in filter(lambda x: x is not container, containers):
                    if item in other:
                        break
                else:
                    output[item_path] = container[item]
                    set_ = True
                if item_path not in output:
                    output[item_path] = EOGroup()
                output[item_path].attrs.update(container[item_path].attrs)
                if not set_:
                    _merge(*(c[item] for c in containers if item in c), output=output)  # type: ignore[arg-type]
        return output

    with contextlib.ExitStack() as stack:
        for product in products:
            if product.store is not None:
                stack.enter_context(product, mode="r")
        product = _merge(*products, output=EOProduct(product_name))
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
