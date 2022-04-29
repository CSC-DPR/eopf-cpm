from typing import Any

import dask.array as da
import numpy as np

from eopf.computing.abstract import ProcessingUnit
from eopf.computing.steps import FlagEvaluationStep, InterpolateTpStep, RadToReflStep
from eopf.product import EOProduct
from eopf.product.conveniences import init_product, open_store
from eopf.product.core.eo_group import EOGroup
from eopf.product.core.eo_variable import EOVariable


class OlciL2FinalisingUnit(ProcessingUnit):
    """
    Very preliminary finalisation step.
    Should implement the addition of coordinate variables.
    """

    def run(self, l2_raw: EOProduct, **kwargs: Any) -> EOProduct:  # type: ignore[override]
        target_product = init_product("S3A_OL_2_LFR")
        # copy variables from raw L2 to final output
        l2_measurements: EOGroup = l2_raw.measurements  # type: ignore[assignment]
        target_measurements: EOGroup = target_product.measurements  # type: ignore[assignment]
        for n, v in l2_measurements.variables:
            target_measurements[n] = v
        return target_product


class OlciL2LandUnit(ProcessingUnit):
    """
    Very prototypical land processing unit of the OLCI Level 2 processor.
    """

    def run(self, refl: EOProduct, **kwargs: Any) -> EOProduct:  # type: ignore[override]
        return refl


class OlciL2PreProcessingUnit(ProcessingUnit):
    """
    Example processing unit implementation to demonstrate the chaining of processing units
    and of processing steps.
    """

    def run(self, l1b: EOProduct, **kwargs: Any) -> EOProduct:  # type: ignore[override]
        """
        Processing unit implementation method with three steps
          - pixel classification,
          - sun zenith angle interpolation,
          - radiance to reflectance conversion.
        :param inputs: l1b: the OLCI L1B input product
        :param parameters: processing parameters, not used by this processing unit
        :return: reflectance product
        """

        # create output, yet empty
        result = EOProduct("OLCI_L2_pre")
        result.add_group("measurements")
        with open_store(l1b):
            # compute valid mask
            quality_flags: EOVariable = l1b.quality.image.quality_flags  # type: ignore[attr-defined]
            valid_mask_step = FlagEvaluationStep(quality_flags)
            l1b_valid_mask = valid_mask_step.apply(quality_flags, flag_expression="NOT invalid")

            # read solar flux and SZA required for rad2refl conversion
            solar_flux = l1b.conditions.instrument.solar_flux.compute()  # type: ignore[attr-defined]
            sza_tp = l1b.conditions.geometry.sza.compute()  # type: ignore[attr-defined]

            # Interpolate SZA from tie-point grid to image grid
            interpolate_tp_step = InterpolateTpStep()
            detector_index = l1b.coordinates.image_grid.detector_index  # type: ignore[attr-defined]
            block_pattern = da.zeros(shape=detector_index.data.blocks.shape, chunks=(1, 1), dtype=np.byte)
            sza = interpolate_tp_step.apply(
                block_pattern,
                tp_data=sza_tp,
                tp_step=(1, 64),  # TBD calc from input
                image_shape=detector_index.shape,
                image_chunksize=detector_index.data.chunksize,
            )

            # convert radiance to reflectance for each radiance band
            rad_to_refl_step = RadToReflStep()
            result_measurements: EOGroup = result.measurements  # type: ignore[assignment]
            for band in range(21):
                bandKey = str(band + 1).rjust(2, "0")
                band_name = f"oa{bandKey}_radiance"
                radiance_var = l1b.measurements.image[band_name]  # type: ignore[attr-defined]
                radiance_dask_array = radiance_var
                reflectance = rad_to_refl_step.apply(
                    radiance_dask_array,
                    l1b_valid_mask,
                    detector_index,
                    sza,
                    solar_flux=solar_flux[band],
                    scale_factor=radiance_var.attrs["scale_factor"],
                    add_offset=radiance_var.attrs["add_offset"],
                    fill_value=radiance_var.attrs["_FillValue"],
                )
                target_band_name = f"oa{bandKey}_reflectance"
                result_measurements[target_band_name] = reflectance

            return result
