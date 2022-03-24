from typing import TYPE_CHECKING

from eopf.product.store.rasterio import EORasterIOAccessor

if TYPE_CHECKING:
    from eopf.product.core.eo_object import EOObject


class FlagAccessor(EORasterIOAccessor):
    """
    Accessor representation to access Raster Flag object.

    Parameters
    ----------
    url: str
        path or url to access
    flag_values: str, optional
        flag_values in sense of the CF Convention
    flag_meanings: str, optional
        flag_meanings in sense of the CF Convention
    flag_masks: str, optional
        flag_masks in sense of the CF Convention

    Attributes
    ----------
    url: str
        path or url to access
    flag_values: str, optional
        flag_values in sense of the CF Convention
    flag_meanings: str, optional
        flag_meanings in sense of the CF Convention
    flag_masks: str, optional
        flag_masks in sense of the CF Convention
    """

    def __init__(self, url: str, flag_values: str = "", flag_meanings: str = "", flag_masks: str = "") -> None:
        super().__init__(url)
        self.flag_values = tuple(flag_values.split(","))
        self.flag_masks = tuple(i.encode() for i in flag_masks.split(","))
        self.flag_meanings = flag_meanings.replace(",", " ")

    def __getitem__(self, key: str) -> "EOObject":
        based_variables = super().__getitem__(key)
        if self.flag_values:
            based_variables.attrs["flag_values"] = self.flag_values
        if self.flag_masks:
            based_variables.attrs["flag_masks"] = self.flag_masks
        based_variables.attrs["flag_meanings"] = self.flag_meanings
        return based_variables
