class AlreadyClose(Warning):
    """When a store is already close"""


class AlreadyOpen(Warning):
    """When a store is already open"""


class NoLoggingConfigurationFile(Warning):
    """When the preset/given logging configuration file does not contain any .conf or yaml file"""


class DaskProfilerHtmlDisplayNotWorking(Warning):
    """When the report display of the dask_profiler is not working"""


class FormatterAlreadyRegistered(Warning):
    """When a formatter with the same name was already registered"""
