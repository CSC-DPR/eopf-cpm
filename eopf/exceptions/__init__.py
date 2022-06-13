class EOObjectExistError(Exception):
    """Raised by `EOContainer` when you redefine an existing key"""


class EOObjectMultipleParentError(Exception):
    """Raised by `EOObject` with already set parent and
    manipulate them in context with an other one"""


class InvalidProductError(Exception):
    """Raised when trying to manipulate a product without valid requirements"""


class MissingConfigurationParameter(Exception):
    """Raised when object configuration is not set"""


class StoreNotDefinedError(Exception):
    """Raised when store is None in the given context"""


class StoreNotOpenError(Exception):
    """Raised when access to a closed store"""


class XmlParsingError(Exception):
    """Raised when xml have different structure has expected"""


class LoggingConfigurationDirDoesNotExist(Exception):
    """When the preset or given logging directory does not exist"""


class LoggingConfigurationAlreadyExists(Exception):
    """When a logging configuration with the same name is already registered"""


class LoggingConfigurationFileTypeNotSupported(Exception):
    """When the logging file name does not have a .conf or .yaml extension"""


class LoggingConfigurationNotRegistered(Exception):
    """When a given logging configuration name is not registered"""


class LoggingConfigurationFileIsNotValid(Exception):
    """When a given logging configuration file .conf/.yaml can applyed"""


class DaskProfilerError(Exception):
    """When the dask_profiler raises any error"""


class SingleThreadProfilerError(Exception):
    """When the single_thread_profiler raises any error"""


class FormattingError(Exception):
    """When a formatter raises exceptions"""


class FormattingDecoratorMissingUri(Exception):
    """When the decorated function does not contain an argument path, url or key"""


class XmlManifestNetCDFError(Exception):
    """When trying to compile the manifest from NetCDF data (s3)"""
