
How to use EOProduct
====================

.. jupyter-execute::
    :hide-output:
    :hide-code:

    import numpy as np

Every component to work with product are given in the module :py:mod:`eopf.product`.

The main object class is the :py:class:`~eopf.product.core.eo_product.EOProduct`, a dict-like class that aggregate group object,
named :py:class:`~eopf.product.core.eo_group.EOGroup`, and are compliant with `Common data model`_.

Create a product
----------------

    :py:class:`~eopf.product.core.eo_product.EOProduct` is class that have only one mandatory parameter corresponding to the product name
    to identify it.

        .. jupyter-execute::

            from eopf.product import EOProduct
            empty_product = EOProduct("empty_product")
            empty_product

    :py:class:`~eopf.product.core.eo_product.EOProduct` must follow a specific format to be `valid`, so our `empty_product` is not a valid one:

        .. jupyter-execute::

            empty_product.is_valid()

    To become a `valid` one, you must add **measurements** and **coordinates** groups:

        .. jupyter-execute::

            empty_product.add_group("measurements")
            empty_product.add_group("coordinates")
            empty_product.is_valid()

    The :py:mod:`eopf.product.conveniences` provide a function
    :py:func:`~eopf.product.conveniences.init_product` to help you to create a valid product.

        .. jupyter-execute::

            from eopf.product.conveniences import init_product
            product = init_product("product_initialized")
            product

    Now if we check the validity of our newly created product, it must be :obj:`True`

        .. jupyter-execute::

            product.is_valid()


Groups and Variables
--------------------

Here we describe how to interact with :py:class:`~eopf.product.core.eo_product.EOProduct`,
:py:class:`~eopf.product.core.eo_variable.EOVariable` and :py:class:`~eopf.product.core.eo_group.EOGroup`.
To learn more about :py:class:`~eopf.product.core.eo_variable.EOVariable`, you can go to :ref:`eovariable-usage`.

Accessing Groups and Variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


    When you use :py:class:`~eopf.product.core.eo_product.EOProduct`,
    you can add :py:class:`~eopf.product.core.eo_group.EOGroup` (resp. :py:class:`~eopf.product.core.eo_variable.EOVariable`) from different way.

    The first one is to simply add them from the top level product,
    using method :py:meth:`~eopf.product.core.eo_product.EOProduct.add_group`
    (resp. :py:meth:`~eopf.product.core.eo_product.EOProduct.add_variable`).

        .. warning::
            :py:class:`~eopf.product.core.eo_product.EOProduct` does not support variable at top level.

            .. jupyter-execute::
                :raises: InvalidProductError

                product.add_variable("my_variable", data=[1,2,3])

        .. jupyter-execute::

            product.add_group("measurements/image")


    When you provide a full path of group, if some of them not exists, we create them.

        .. jupyter-execute::
            :hide-output:

            # We create both image and radiance
            product.add_group("measurements/image/radiance")

        You can also mention dimensions name as named parameter

        .. jupyter-execute::
            :hide-output:

            data = np.random.sample((100, 100))
            # We create both reflectance and oa01_reflectance.
            product.add_variable("measurements/image/reflectance/oa01_reflectance", dims=["longitude", "latitude"], data=data)

    :py:class:`~eopf.product.core.eo_product.EOProduct` are dict-like object (i.e :py:class:`~collections.abc.MutableMapping`),
    so you can retrieve your group by index, with the fullpath for exemple, or directly with a `dot`:

        .. code-block:: python

            product["measurements"]
            product["measurements/image/radiance"]
            product.measurements.image

        .. code-block:: python

            product["measurements/image/reflectance/oa01_reflectance"]
            product.measurements.image.reflectance.oa01_reflectance


    :py:class:`~eopf.product.core.eo_group.EOGroup` are similar to :py:class:`~eopf.product.core.eo_product.EOProduct`,
    and you can retrieve or create sub :py:class:`~eopf.product.core.eo_group.EOGroup` (resp. :py:class:`~eopf.product.core.eo_variable.EOVariable`) from them:

        .. jupyter-execute::
            :hide-output:

            latitude = np.random.sample((100,100))
            longitude = np.random.sample((100,100))

            product["coordinates"].add_group("orphans")

            product.coordinates["orphans"].add_variable("latitude", data=latitude)
            product["coordinates"].add_variable("orphans/longitude", data=longitude)

    If you have a sub :py:class:`~eopf.product.core.eo_group.EOGroup`, and you want to retrieve or add an other one higher in the hierarchy,
    you can path an asbolute path from the top level product:

        .. jupyter-execute::

            subgroup = product.measurements["image"]
            new_group_higher = subgroup.add_group("/conditions/geometry")
            product["/conditions/geometry"] == new_group_higher

    The last option to create a group is by directly indexing one in the :py:class:`~eopf.product.core.eo_product.EOProduct` or :py:class:`~eopf.product.core.eo_group.EOGroup`

        .. jupyter-execute::

            from eopf.product.core import EOGroup
            product["conditions/meteo"] = EOGroup()
            product["conditions/meteo"]

        .. jupyter-execute::

            radiance_data = np.random.sample((100,100))

            from eopf.product.core import EOVariable
            product["measurements/image"]["oa02_radiance"] = EOVariable(data=radiance_data)
            product["measurements/image"]["oa02_radiance"]

    .. note::

        For :py:class:`~eopf.product.core.eo_variable.EOVariable` data must be an object usable by :py:class:`~xarray.DataArray`

Iterate over Groups
~~~~~~~~~~~~~~~~~~~

As a dict-like object, :py:class:`~eopf.product.core.eo_group.EOGroup` are iterable,
also, to iterate over specific subitems we provide two property on
:py:class:`~eopf.product.core.eo_group.EOGroup` and :py:class:`~eopf.product.core.eo_product.EOProduct`:

    * :py:attr:`~eopf.product.core.eo_container.EOContainer.groups`
    * :py:attr:`~eopf.product.core.eo_container.EOContainer.variables`

    .. jupyter-execute::

        for group_name, eogroup in product.groups:
            for subgroup_name, _ in eogroup.groups:
                print(f"group in {group_name}: {subgroup_name}")

        for group_name, eogroup in product.measurements.groups:
            for subvar_name, _ in eogroup.variables:
                print(f"variable in {group_name}: {subvar_name}")


Coordinates
-----------

    Coordinates are determined by dimensions and retrieved from :py:attr:`~eopf.product.core.eo_product.EOProduct.coordinates` field

    .. jupyter-execute::

        data_coord_latitude = np.random.sample((100,100))

        product["coordinates/orphans/latitude"] = EOVariable(data=data_coord_latitude, dims=["longitude", "latitude"])
        product["measurements/image/reflectance/oa01_reflectance"].coordinates

Attibutes
---------

    :py:class:`~eopf.product.core.eo_product.EOProduct`, :py:class:`~eopf.product.core.eo_group.EOGroup` and :py:class:`~eopf.product.core.eo_variable.EOVariable` have a field named **attrs**, a dict object, that
    contained all attributes of the class compliant with the CF Convention.

    .. jupyter-execute::

        product.attrs["bbox"] = [52.455, 3.16201, 39.5462, 23.1664]
        product.attrs["id"] = "S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3"
        product.attrs["product_type"] = "S3_OL_1_EFR"
        product.attrs

    .. jupyter-execute::

        group = product["conditions"]
        group.attrs["meteo"] = {'source': 'ECMWF', 'type': 'ANALYSIS', 'time_relevance': 0}
        group.attrs["orbit_reference"] = {
            'absolute_pass_number': 61618,
            'relative_pass_number': 72,
            'cycle_number': 81,
            'phase_identifier': 1,
        }
        group.attrs

    .. jupyter-execute::

        variable = product["measurements/image/oa02_radiance"]
        variable.attrs["ancillary_variables"] = "Oa02_radiance_err"
        variable.attrs["coordinates"] = "time_stamp altitude latitude longitude"
        variable.attrs

Product Type and short names
----------------------------

    Product can be describe with a specific code that you can retrieve with
    :py:attr:`~eopf.product.core.eo_product.EOProduct.product_type`

    .. jupyter-execute::

        product.product_type

    each known type of product have a list of :py:attr:`~eopf.product.core.eo_product.EOProduct.short_names`,
    which are helpful to retrieve variables from product.

    .. jupyter-execute::

        product.oa02_radiance


Tree of the product
-------------------

    :py:class:`~eopf.product.core.eo_product.EOProduct` have a tree function that can be used to display a tree.
    If you are in a :obj:`jupyter` environment, an interactive version is displayed.

    .. jupyter-execute::

        product.tree()

    .. jupyter-execute::
        :hide-code:

        for name, group in product._groups.items():
            print(f"├── {name}")
            product._create_structure(group, level=1)

Reading a Product from a store
------------------------------

    .. jupyter-execute::
        :hide-output:
        :hide-code:

        file_name = "docs/source/_data/S3_OL_1_EFR.zarr"


    To read data of a product, from a specific format, you must instantiate your :obj:`eopf.product.EOProduct` with
    the parameter **storage**, that can be a :obj:`str` or a :py:class:`~eopf.product.store.abstract.EOProductStore`.

    .. jupyter-execute::

        from eopf.product.store import EOZarrStore

        product_read_from_store = EOProduct("product_read", storage=EOZarrStore(file_name))

    .. note::
        The default type when you provide a :obj:`str` is a :py:class:`~eopf.product.store.zarr.EOZarrStore`

    So now if you access to an elements of your product, it come from the zarr file.

    .. warning::
        You have to **open** your store before, using :py:meth:`~eopf.product.core.eo_product.EOProduct.open` or :py:func:`~eopf.product.conveniences.open_store`

    .. jupyter-execute::

        from eopf.product.conveniences import open_store

        with open_store(product_read_from_store, mode='r'):
            product_read_from_store["measurements/image"]

    .. jupyter-execute::

        with open_store(product_read_from_store, mode='r'):
            print(product_read_from_store["measurements/image/oa02_radiance"].data)
            print(product_read_from_store["coordinates/tiepoint_grid/latitude"].data)
            print(product_read_from_store["measurements/image/oa02_radiance"].compute())
            print(product_read_from_store["coordinates/tiepoint_grid/latitude"].compute())


    If you want to load a full product in memory, you can use the :py:meth:`~eopf.product.core.eo_product.EOProduct.load` method:

    .. jupyter-execute::

        with open_store(product_read_from_store):
            product_read_from_store.load()
        product_read_from_store["measurements/image/"]


Writting Products
-----------------

    Writting is pretty similar, but you have to use the :py:meth:`~eopf.product.core.eo_product.EOProduct.write` method

    .. jupyter-execute::

        output_folder = "output"
        output_filename = "S3_OL_1_EFR.zarr"
        with product.open(mode="w", storage=EOZarrStore(f"{output_folder}/{output_filename}")):
            product.write()

    .. warning::
        You have to **open** your store before, using :py:meth:`~eopf.product.core.eo_product.EOProduct.open` or :py:func:`~eopf.product.conveniences.open_store`


.. _Common data model: https://docs.unidata.ucar.edu/netcdf-c/current/netcdf_data_model.html
