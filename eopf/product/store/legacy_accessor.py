from eopf.product.core import EOVariable

from .extract_dim import EOExtractDimAccessor
from .rasterio import EORasterIOAccessor


class JP2YAccessor(EOExtractDimAccessor):
    """
    Store representation to Extract Y Dimension from EORasterIOAccessor

    Parameters
    ----------
    url: str
        path or url to access

    Attributes
    ----------
    url: str
        path or url to access
    """

    _store_cls = EORasterIOAccessor
    _extract_dim = "y"


class JP2XAccessor(EOExtractDimAccessor):
    """
    Store representation to Extract X Dimension from EORasterIOAccessor.

    Parameters
    ----------
    url: str
        path or url to access

    Attributes
    ----------
    url: str
        path or url to access
    """

    _store_cls = EORasterIOAccessor
    _extract_dim = "x"


class JP2SpatialRefAccessor(EOExtractDimAccessor):
    """
    Store representation to Construct Spatial Ref Grid mapping variable
    in sense of the CF convention.

    Parameters
    ----------
    url: str
        path or url to access

    """

    _store_cls = EORasterIOAccessor
    _extract_dim = "spatial_ref"

    def __getitem__(self, key: str) -> EOVariable:

        eo_obj = super().__getitem__(key)
        if isinstance(eo_obj, EOVariable):
            return EOVariable(attrs=eo_obj.attrs)
        return eo_obj
