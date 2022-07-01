import json
import logging
import os
from typing import Any, Optional

from eopf.computing.abstract import EOProcessor
from eopf.exceptions import InvalidProductError
from eopf.product.core.eo_product import EOProduct
from eopf.qualitycontrol.eo_qc_config import EOPCConfigFactory, EOQCConfig

logger = logging.getLogger("EOPF")


class EOQCProcessor(EOProcessor):
    """Main class of the quality control module

    Parameters
    ----------
    identifier: int
        The identifier.
    config_path: str
        The quality control configuration to run. By default it's None.

    Attributes
    ----------
    qc_config: EOQCConfig
        The quality control configuration who will be executed.
    """

    def __init__(self, identifier: Optional[int] = None, config_path: Optional[str] = None) -> None:
        super().__init__(identifier=identifier)
        self.qc_config: Optional[EOQCConfig] = None
        if config_path is not None:
            self.set_config(config_path=config_path)

    def set_config(self, config_path: str) -> None:
        """A qc_config setter.

        Parameters
        ----------
        config_path: str
            The path to the configuration.
        """
        self.qc_config = EOQCConfig(config_path=config_path)

    def run(  # type: ignore[override]
        self,
        eoproduct: EOProduct,
        update_attrs: bool = True,
        write_report: bool = False,
        report_path: Optional[str] = None,
        config_path: Optional[str] = None,
        **kwargs: Any,
    ) -> EOProduct:
        """Execute all checks of the default configuration for a EOProduct.

        Parameters
        ----------
        eoproduct: EOProduct
            EOProduct to check
        update_attrs: bool = True
            To write the result of the checks in quality attribute groupp of the EOproduct. By default it's True.
        write_report: bool = False
            To write the report of the quality control processor. By default it's False.
        report_path: str = None
            The path where to write the quality control report. By default it's None.
        config_path: Optional[str] = None
            The quality control configuration to run. By default it's None and it load the default configuration.
        **kwargs: any
            any needed kwargs

        Returns
        -------
        EOProduct
            The controlled product.
        """
        # If no given configuration in Processor, it get the default one.
        if not eoproduct.type:
            raise InvalidProductError(f"Missing product type for {eoproduct.name} in {self}")
        qc_config = EOQCConfig(config_path) if config_path else self.qc_config
        if qc_config is None:
            qc_config = EOPCConfigFactory().get_default(eoproduct.type)
        # Run check(s) of the configuration
        for qc in qc_config.qclist.values():
            try:
                qc.check(eoproduct)
            except Exception as e:
                logger.exception(f"An erreur ocurred in : {qc.id}", e)
                raise e
        # If true it update the quality attribute of the product.
        if update_attrs:
            self.update_attributs(eoproduct, qc_config)
        # If the it write the quality control report in a .json file.
        if write_report:
            if report_path is not None:
                self.write_report(eoproduct, report_path, qc_config)
            else:
                raise ValueError("Can't write report no path given")
        return eoproduct

    def update_attributs(self, eoproduct: EOProduct, qc_config: EOQCConfig) -> None:
        """This method update the EOProduct quality group attributes with the result of quality control.
        Parameters
        ----------
        eoproduct: EOProduct
            EOProduct to check
        config_path: EOQCConfig
            The quality control configuration which was used.
        """
        if "quality" not in eoproduct:
            eoproduct.add_group("quality")
        if "qc" not in eoproduct.quality:  # type: ignore[operator]  # Quality is a EOGroup
            eoproduct.quality.attrs["qc"] = {}
        for qc in qc_config.qclist.values():
            if qc.status:
                eoproduct.quality.attrs["qc"][qc.id] = {
                    "version": qc.version,
                    "status": qc.status,
                    "message": qc.message_if_passed,
                }
            else:
                eoproduct.quality.attrs["qc"][qc.id] = {
                    "version": qc.version,
                    "status": qc.status,
                    "message": qc.message_if_failed,
                }

    def write_report(self, eoproduct: EOProduct, report_path: str, qc_config: EOQCConfig) -> bool:
        """This method write the quality control report in json in given location.

        Parameters
        ----------
        eoproduct: EOProduct
            EOProduct to check
        report_path: str = None
            The path where to write the qc report.
        config_path: EOQCConfig
            The quality control configuration which was used.

        Returns
        -------
        bool
            Has the quality control report been successfully written, true is ok, false if not.
        """
        report_path = os.path.join(report_path, f"QC_report_{eoproduct.name}.json")
        report: dict[str, Any] = {}
        report["Product_name"] = eoproduct.name
        report["Product_type"] = eoproduct.type
        report["Acquisition_station"] = "To be defined"
        report["Processing_center"] = "To be defined"
        report["Start_sensing_time"] = "To be defined"
        report["Stop_sensing_time"] = "To be defined"
        report["Relative_orbit_number"] = "To be defined"
        report["Absolute_orbit_number"] = "To be defined"
        report["Inspection_date"] = "To be defined"
        report["Inspection_time"] = "To be defined"
        for qc in qc_config.qclist.values():
            if qc.status:
                report[qc.id] = {"version": qc.version, "status": qc.status, "message": qc.message_if_passed}
            else:
                report[qc.id] = {"version": qc.version, "status": qc.status, "message": qc.message_if_failed}
        try:
            with open(report_path, "w") as outfile:
                json.dump(report, outfile, indent=4)
                return True
        except Exception as e:
            raise e
