class EOObjectExistError(Exception):
    """Raised by `EOContainer` when you redefine an existing key"""


class EOObjectMultipleParentError(Exception):
    """Raised by `EOObject` with already set parent and
    manipulte them in context with an other one"""


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
