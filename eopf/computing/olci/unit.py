from typing import Any

from eopf.computing.abstract import ProcessingUnit
from eopf.computing.general import ExtractVariableProcessingUnit, IdentityProcessingUnit
from eopf.computing.steps import FlagEvaluationStep, InterpolateTpStep, RadToReflStep
from eopf.product import EOProduct
from eopf.product.conveniences import open_store
from eopf.product.core.eo_group import EOGroup
from eopf.product.core.eo_variable import EOVariable


class OlciL2FinalisingUnit(ExtractVariableProcessingUnit):
    """
    Very preliminary finalisation step.
    Should implement the addition of coordinate variables.
    """

    _CONTAINER_VARIABLES_PATHS = ["/measurements"]


class OlciL2LandUnit(IdentityProcessingUnit):
    """
    Very prototypical land processing unit of the OLCI Level 2 processor.
    """


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
            valid_mask_step = FlagEvaluationStep()
            l1b_valid_mask = valid_mask_step.apply(quality_flags, flag_expression="NOT invalid")

            # read solar flux and SZA required for rad2refl conversion
            solar_flux = l1b.conditions.instrument.solar_flux.compute()  # type: ignore[attr-defined]
            sza_tp = l1b.conditions.geometry.sza.compute()  # type: ignore[attr-defined]

            # Interpolate SZA from tie-point grid to image grid
            interpolate_tp_step = InterpolateTpStep()
            detector_index = l1b.coordinates.image_grid.detector_index  # type: ignore[attr-defined]
            sza = interpolate_tp_step.apply(
                sza_tp,
                tp_step=(1, 64),  # TBD calc from input
                shape=detector_index.shape,
                chunksize=detector_index.data.chunksize,
            )

            # convert radiance to reflectance for each radiance band
            rad_to_refl_step = RadToReflStep()
            result_measurements: EOGroup = result.measurements  # type: ignore[assignment]
            for band in range(21):
                band_name = f"oa{band+1:02}_radiance"
                radiance_var = l1b.measurements.image[band_name]  # type: ignore[attr-defined]
                reflectance = rad_to_refl_step.apply(
                    radiance_var,
                    l1b_valid_mask,
                    detector_index,
                    sza,
                    solar_flux=solar_flux[band],
                )
                target_band_name = band_name.replace("radiance", "reflectance")
                result_measurements[target_band_name] = reflectance

            return result
