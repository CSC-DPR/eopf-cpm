class AlreadyClose(Warning):
    """When a store is already close"""


class AlreadyOpen(Warning):
    """When a store is already open"""


class NoLoggingConfigurationFile(Warning):
    """When the preset/given logging configuration file does not contain any .conf or yaml file"""


class DaskProfilerHtmlDisplayNotWorking(Warning):
    """When the report display of the dask_profiler is not working"""


class LoggingLevelIsNoneStandard(Warning):
    """When the given log level is set to a value which is none Python standard"""
