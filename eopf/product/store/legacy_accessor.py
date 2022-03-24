from typing import TYPE_CHECKING

from .extract_dim import EOExtractDimAccessor
from .rasterio import EORasterIOAccessor

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core import EOVariable


class EOJP2YAccessor(EOExtractDimAccessor):
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

    # docstr-coverage: inherited
    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return file_path.endswith(".jp2")


class EOJP2XAccessor(EOExtractDimAccessor):
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

    # docstr-coverage: inherited
    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return file_path.endswith(".jp2")


class EOJP2SpatialRefAccessor(EOExtractDimAccessor):
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

    def __getitem__(self, key: str) -> "EOVariable":
        from eopf.product.core import EOVariable

        eo_obj = super().__getitem__(key)
        if isinstance(eo_obj, EOVariable):
            return EOVariable(attrs=eo_obj.attrs)
        return eo_obj

    # docstr-coverage: inherited
    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return file_path.endswith(".jp2")
