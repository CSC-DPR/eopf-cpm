"""The objectif of the eopf.qualitycontrol module is to perform systematic quality control inspection
of the EOProducts received as input, providing as output an EOPF-QC report.

The  eopf.qualitycontrol component is designed to be used in the following modes:

    * As a standalone post-processor that can be executed by the master process as an independent processor.
    * As python module that can be imported and used by other processors or inside the triggering module
      just before the final output generation step.

It  performs a series of automated basic checks which result in passed/failed criteria.
The checks results are gathered in an EOPF-QC report, which is the main output of the eopf.qualitycontrol module.

The EOPF-QC report is a part of the EOProduct by default for the zarr format.
However, this feature is configurable and it may also be provided as a standalone report.

The eopf.qualitycontrol component comes with common quality checks for all product types.

The eopf.qualitycontrol component generates an EOPF-QC report in json format.

The EOPF-QC report contains for each inspected product:

    * The file name of the inspected product
    * The filet type of the inspected product
    * The processing center
    * The processing time of the product
    * The start/stop sensing time
    * The orbit numbers (absolute and relative)
    * The date and time of the inspection
    * The software version of the QC component
    * The name and version of each check
    * The status of each check
    * A summary of statistical information about checks
    * Plots of key EOProduct’s variables
"""
