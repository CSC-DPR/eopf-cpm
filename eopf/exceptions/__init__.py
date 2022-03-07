class InvalidProductError(Exception):
    pass


class StoreNotDefinedError(Exception):
    pass


class StoreNotOpenError(Exception):
    pass


class EOObjectExistError(Exception):
    pass


class EOObjectMultipleParentError(Exception):
    pass


class XmlParsingError(Exception):
    pass


class MissingConfigurationParameter(Exception):
    pass
