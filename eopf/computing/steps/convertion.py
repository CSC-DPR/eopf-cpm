import math
from numbers import Number
from typing import Any, Optional

import numpy as np

from eopf.computing.abstract import BlockProcessingStep


class RadToReflStep(BlockProcessingStep):
    """
    Radiance to reflectances conversion example processing step implementation
    to demonstrate the chaining of processing steps.
    """

    def func(  # type: ignore[override]
        self,
        radiance: np.ndarray[Any, np.dtype[Any]],
        valid_mask: np.ndarray[Any, np.dtype[Any]],
        detector_index: np.ndarray[Any, np.dtype[Any]],
        sza: np.ndarray[Any, np.dtype[Any]],
        solar_flux: np.ndarray[Any, np.dtype[Any]] = np.array(()),
        scale_factor: Optional[Number] = None,
        add_offset: Optional[Number] = None,
        fill_value: Optional[Number] = None,
    ) -> np.ndarray[Any, np.dtype[Any]]:
        """
        Block-wise radiance to reflectance conversion using solar flux and sza.
        :param inputs: list of input block numpy arrays
                       radiance: block of radiance, one band
                       valid_mask: block of boolean mask
                       detector_index: block of detector index per pixel
                       sza: block of sun zenith angles, interpolated to image coordinates
        :param kwargs: solar_flux: 1-D numpy array, LUT from detector_index to solar flux
                       scale_factor: scale factor to be applied to radiance
                       add_offset: offset to be applied to radiance
                       _FillValue: invalid data in radiance before scale and offset is applied
        :return: numpy array with reflectance for the block
        """
        if any(x is None for x in [scale_factor, add_offset, fill_value]):
            raise ValueError(f"Missing kwargs : {scale_factor=}, {add_offset=}, {fill_value=}.")

        # TODO remove after types are preserved by reader
        if detector_index.dtype == np.float64:
            detector_index = np.array(detector_index, dtype=np.uint16)

        # mask out inputs according to valid_mask and _FillValue
        valid_mask[(radiance == fill_value)] = False
        radiance[(not valid_mask)] = np.nan
        detector_index[(not valid_mask)] = 0

        # convert radiance to reflectance using solar flux and sza, apply scale and offset to radiance
        result = (
            math.pi
            * (radiance * scale_factor + add_offset)
            / solar_flux[detector_index]
            / np.cos(math.pi * sza / 180.0)
        )
        return result
