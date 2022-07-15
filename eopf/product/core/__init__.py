"""The core package contains classes allowing to define the harmonized data structure
common to all Sentinel products, the main class of this package representing
this structure is the EOProduct class; it follows the Common Data Model design.
So, an EOProduct is a container of EOGroup which itself is a container of EOVariable.

Three abstract classes, EOAbstract, EOObject and EOContainer are used as interfaces
to define the properties and behaviors common to the EOProduct, EOGroup and EOVariable classes.

Thus, EOGroup and EOVariable are two classes sharing the same properties inherited
from the EOAbstract and EOObject interfaces.
The same logic is applied for the EOProduct and EOGroup classes, they share the same properties
and behaviors inherited from the EOAbstract and EOContainer interfaces.
"""
from .eo_group import EOGroup
from .eo_product import EOProduct
from .eo_variable import EOVariable

__all__ = ["EOGroup", "EOProduct", "EOVariable"]
