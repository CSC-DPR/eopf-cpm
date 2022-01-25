from typing import Optional, Union

from eopf.product import EOProduct
from eopf.product.store.abstract import EOProductStore


def init_product(product_name: str, *, store_or_path_url: Optional[Union[str, EOProductStore]] = None):
    product = EOProduct(product_name, store_or_path_url=store_or_path_url)

    for group_name in product.MANDATORY_FIELD:
        product.add_group(group_name)
    return product
