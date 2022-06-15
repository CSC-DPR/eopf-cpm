import json
import logging
import os
from typing import Any, Optional

from eopf.computing.abstract import EOProcessor
from eopf.product.core.eo_product import EOProduct
from eopf.qualitycontrol.eo_qc_config import EOPCConfigFactory, EOQCConfig

logger = logging.getLogger("EOPF")


class EOQCProcessor(EOProcessor):
    """Main class of the quality control module

    Parameters
    ----------
    eoproduct: Optional[EOProduct]
        The product to control, by default it's None.
    qc_list: Optional[EOQCConfig]
        A list of configuration to execute.


    Attributes
    ----------
    qc_config: EOQCConfig
        The quality control configuration who will be executed.
    eoproduct: EOProduct
        The eoproduct who will be controled.
    qc_configfactory : EOPCConfigFactory
        The quality control configuration factory which contains the quality control configuration.
    """

    def __init__(self, eoproduct: Optional[EOProduct] = None, qc_list: Optional[EOQCConfig] = []) -> None:
        self.qc_config = None
        self.eoproduct = eoproduct
        self.qc_configfactory = EOPCConfigFactory()
        for qc in qc_list:
            self.qc_configfactory.add_qc_config(qc)

    def run(
        self,
        eoproduct: EOProduct,
        update_attrs: bool = True,
        write_report: bool = False,
        report_path: str = None,
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
        **kwargs: any
            any needed kwargs

        Returns
        -------
        EOProduct
            The controlled product.
        """
        self.eoproduct = eoproduct
        self.qc_config = self.qc_configfactory.get_default(eoproduct.type)
        for qc in self.qc_config.qclist().values():
            qc.check(self.eoproduct)
        if update_attrs:
            self.update_attributs()
        if write_report:
            if report_path is not None:
                if not self.write_report(report_path):
                    logger.error("Error while writing report")
            else:
                logger.error("Can't write report no path given")
        return self.eoproduct

    def update_attributs(self) -> None:
        """This method update the EOProduct quality group attributes with the result of quality control."""
        if "quality" not in self.eoproduct:
            self.eoproduct.add_group("quality")
        if "qc" not in self.eoproduct.quality:
            self.eoproduct.quality.attrs["qc"] = {}
        for qc in self.qc_config.qclist().values():
            self.eoproduct.quality.attrs["qc"][qc.id] = {"version": qc.version, "status": bool(qc.status)}

    def write_report(self, report_path: str) -> bool:
        """This method write the quality control report in json in given location.

        Parameters
        ----------
        report_path: str = None
            The path where to write the qc report.

        Returns
        -------
        bool
            Has the quality control report been successfully written, true is ok, false if not.
        """
        report_path = os.path.join(report_path, "QC_report_" + self.eoproduct.name + ".json")
        report = {}
        report["Product_name"] = self.eoproduct.name
        report["Product_type"] = self.eoproduct.type
        for qc in self.qc_config.qclist().values():
            report[qc.id] = {"version": qc.version, "status": bool(qc.status)}
        try:
            with open(report_path, "w") as outfile:
                json.dump(report, outfile, indent=4)
                return True
        except ValueError:
            return False
