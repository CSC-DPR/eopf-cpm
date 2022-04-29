from typing import Any

from eopf.computing.abstract import Processor
from eopf.product import EOProduct

from .unit import OlciL2FinalisingUnit, OlciL2LandUnit, OlciL2PreProcessingUnit


class OlciL2LandProcessor(Processor):
    """
    Prototype implementation of (parts of) the OLCI Level 2 processor.
    Basis of the implementation is the OLCI L2 IPF DPM.
    The example demonstrates the chaining of processing units.
    The current version of the processing unit implements the computation of a valid mask
    and the conversion from radiances to reflectances.
    """

    def run(self, l1b: EOProduct, **kwargs: Any) -> EOProduct:  # type: ignore[override]
        """
        Processor implementation method with three sub-units that transform the OLCI L1 into
        reflectances and masks, and finally into land parameters land variables.
        The current version is implemented up to reflectances. Other steps pass through their inputs.
        :param inputs: dictionary of named input products, "l1b" expected as name of the input
        :param parameters: processing parameters passed to the sub-units
        :return: Level 2 output product for lazy evaluation (requires writing to actually process)
        """
        # The processor uses sub-units
        preprocessing_unit = OlciL2PreProcessingUnit()
        land_processing_unit = OlciL2LandUnit()
        product_finalisation_unit = OlciL2FinalisingUnit()
        # pre-processing
        reflectance_intermediate = preprocessing_unit.run(l1b, **kwargs)
        # land processing
        land_intermediate = land_processing_unit.run(reflectance_intermediate, **kwargs)
        # completion of the EOProduct
        target_product = product_finalisation_unit.run(land_intermediate, **kwargs)

        return target_product
