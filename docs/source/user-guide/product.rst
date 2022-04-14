
How to use EOProduct
====================

.. jupyter-execute::
    :hide-output:
    :hide-code:

    import os
    import shutil
    import numpy as np
    import xarray
    import zarr
    from eopf.product.store import EOZarrStore
    from eopf.product.store.safe import EOSafeStore
    from glob import glob


.. jupyter-execute::
    :hide-output:
    :hide-code:

    input_folder = "/home/cdubos_x/DPR/data"
    output_folder="output"
    output_filename="product_demo_sprint1.zarr"
    try:
        os.mkdir(output_folder)
    except FileExistsError:
        pass
    try:
        shutil.rmtree(f"{output_folder}/{output_filename}")
    except FileNotFoundError:
        pass


Create a product
----------------

    Every component to work with product are given in the module :obj:`eopf.product`.

    The main object class is the :obj:`eopf.product.EOProduct`, a dict-like class that aggregate group object,
    named :obj:`eopf.product.core.EOGroup`, and are compliant with Common data model.

        .. jupyter-execute::

            from eopf.product import EOProduct
            empty_product = EOProduct("empty_product")
            empty_product



    :obj:`eopf.product.EOProduct` must follow a specific format to be `valid`, so our `empty_product` is not a valid one:

        .. jupyter-execute::

            empty_product.is_valid()

    To become a `valid` one, you must add **measurements** and **coordinates** groups:

        .. jupyter-execute::

            empty_product.add_group("measurements")
            empty_product.add_group("coordinates")
            empty_product.is_valid()

    the :obj:`eopf.product.conveniences` provide a simple function :obj:`init_product` to help you to create a valid product.

        .. jupyter-execute::

            from eopf.product.conveniences import init_product
            product = init_product("product_written")
            product

    Now if we check the validity of our newly created product, it must be :obj:`True`

        .. jupyter-execute::

            product.is_valid()


Using groups and variables
--------------------------

    .. jupyter-execute::
        :hide-output:
        :hide-code:

        data_a = np.array([1,1])
        data_b = np.array([2])
        data_c = xarray.DataArray([[3],[3]], dims=["time", "space"])
        data_d = np.array([[4.1],[4.2],[4.3]])
        data_e = xarray.DataArray(np.zeros(10), dims=["dim_group/dim_10"])

        data_coord_time = np.array([1])
        data_coord_space = [2]
        data_coord_dim_10 = xarray.DataArray([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])


    When you use :obj:`eopf.product.EOProduct`, you can add :obj:`eopf.product.core.EOGroup` (resp. :obj:`eopf.product.core.EOVariable`) from different way.
    the first one is to simply add them from the top level product, using :meth:`EOProduct.add_group` (resp. :meth:`EOProduct.add_variable`) method.

        .. warning::
            :obj:`eopf.product.EOProduct` does not support variable at top level.

            .. jupyter-execute::
                :raises: InvalidProductError

                product.add_variable("my_variable", [1,2,3])

        .. jupyter-execute::

            product.add_group("group0")
            product.add_group("measurements/group1", dims=["time", "space"])


    When you provide a full path of group, if some of them not exists, we create them.

        .. jupyter-execute::
            :hide-output:

            product.add_group("measurements/group1/group2/group3") # We create both group2 and group3

        .. jupyter-execute::
            :hide-output:

            product.add_variable("measurements/group1/group2c/variable_d", dims=["c1", "c2"], data=data_d) # We create both group2c and variable_d.

    :obj:`eopf.product.EOProduct` are dict-like object (i.e :obj:`collections.abc.MutableMapping`), so you can retrieve
    your group by index, with the fullpath for exemple, or directly with a `dot`:

        .. jupyter-execute::
            :hide-output:

            product["measurements"]
            product["measurements/group1/group2/group3"]
            product.measurements.group1

        .. jupyter-execute::
            :hide-output:

            product["measurements/group1/group2c/variable_d"]


    :obj:`eopf.product.core.EOGroup` are similar to :obj:`eopf.product.EOProduct`, and you can retrieve or create sub :obj:`eopf.product.core.EOGroup` (resp. :obj:`eopf.product.core.EOVariable`) from them:

        .. jupyter-execute::
            :hide-output:

            product["measurements"].add_group("group1/group2b")

            product.measurements["group1"].add_variable("variable_a", data=data_a)
            product["measurements/group1"].add_variable("group2/variable_b", data=data_b)

    If you have a sub :obj:`eopf.product.core.EOGroup`, and you want to retrieve or add an other one higher in the hierarchy,
    you can path an asbolute path from the top level product:

        .. jupyter-execute::

            subgroup = product.measurements["group1"]
            new_group_higher = subgroup.add_group("/measurements/group1/group2b/group3")
            subgroup["/measurements/group1/group2b/group3"] == new_group_higher

        .. jupyter-execute::

            subgroup.add_variable("/measurements/group1/group2/variable_c", data=data_c, dims=data_c.dims)


    The last option to create a group is by directly indexing one in the :obj:`eopf.product.EOProduct` or :obj:`eopf.product.core.EOGroup`

        .. jupyter-execute::

            from eopf.product.core import EOGroup
            subgroup["sub_new_group"] = EOGroup()
            subgroup["sub_new_group"]

        .. jupyter-execute::

            from eopf.product.core import EOVariable
            product["measurements/group1"]["group2"]["variable_e"] = EOVariable(data=data_e)
            product["measurements/group1"]["group2"]["variable_e"]

    .. note::

        For :obj:`eopf.product.core.EOVariable` data must be an object usable by :obj:`xarray.DataArray`


Coordinates
-----------

    Coordinates are determined by dimensions and retrieved from :obj:`eopf.product.EOProduct.coordinates` field

    .. jupyter-execute::

        product["coordinates/space"] = EOVariable(data=data_coord_space)
        product.coordinates.add_variable("dim_group/dim_10",data=data_coord_dim_10, dims=("space",))
        product.measurements.group1.coordinates

Attibutes
---------

    :obj:`eopf.product.EOProduct`, :obj:`eopf.product.core.EOGroup` and :obj:`eopf.product.core.EOVariable` have a field named **attrs**, a dict object, that
    contained all attributes of the class compliant with the CF Convention.

    .. jupyter-execute::

        product.attrs["33"]=4.2
        product.attrs["test_key"]="test_value"
        product.attrs

    .. jupyter-execute::

        group = product["measurements/group1/group2"]
        group.attrs["33"] = 4.3
        group.attrs["test_key"] = "test_value"
        group.attrs

    .. jupyter-execute::

        variable = product["measurements/group1/group2/variable_b"]
        variable.attrs["33"] = 4.3
        variable.attrs["test_key"] = "test_value"
        variable.attrs

Tree of the product
-------------------

    :obj:`eopf.product.EOProduct` have a tree function that can be used to display a tree.
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

        def write_zarr_file():
            file_name = "file://output/eoproduct_zarr_file.zarr"
            dims = "_EOPF_DIMENSIONS"

            root = zarr.open(file_name, mode="w")
            root.attrs["top_level"] = True
            root.create_group("coordinates")

            root["coordinates"].attrs["description"] = "coordinates Data Group"
            root["coordinates"].create_group("grid")
            root["coordinates"].create_group("tie_point")
            xarray.Dataset({"radiance": ["rows", "columns"], "orphan": ["depths", "length"]}).to_zarr(
                store=f"{file_name}/coordinates/grid",
                mode="a",
            )
            xarray.Dataset({"radiance": ["rows", "columns"], "orphan": ["depths", "length"]}).to_zarr(
                store=f"{file_name}/coordinates/tie_point",
                mode="a",
            )

            root.create_group("measurements")
            root["measurements"].attrs["description"] = "measurements Data Group"
            root["measurements"].create_group("geo_position")
            root["measurements"]["geo_position"].create_group("altitude")
            root["measurements"]["geo_position"].create_group("latitude")
            root["measurements"]["geo_position"].create_group("longitude")

            xarray.Dataset(
                {
                    "polar": xarray.DataArray([[12, 4], [3, 8]], attrs={dims: ["grid/radiance"]}),
                    "cartesian": xarray.DataArray([[5, -3], [-55, 66]], attrs={dims: ["tie_point/orphan"]}),
                },
            ).to_zarr(store=f"{file_name}/measurements/geo_position/altitude", mode="a")
            xarray.Dataset(
                {
                    "polar": xarray.DataArray([[1, 2], [3, 4]], attrs={dims: ["grid/radiance"]}),
                    "cartesian": xarray.DataArray([[9, 7], [-12, 81]], attrs={dims: ["tie_point/orphan"]}),
                },
            ).to_zarr(store=f"{file_name}/measurements/geo_position/latitude", mode="a")
            xarray.Dataset(
                {
                    "polar": xarray.DataArray([[6, 7], [2, 1]], attrs={dims: ["tie_point/radiance"]}),
                    "cartesian": xarray.DataArray([[25, 0], [-5, 72]], attrs={dims: ["grid/orphan"]}),
                },
            ).to_zarr(store=f"{file_name}/measurements/geo_position/longitude", mode="a")
            return file_name

    .. jupyter-execute::
        :hide-output:
        :hide-code:

        file_name = write_zarr_file()


    To read data of a product, from a specific format, you must instantiate your :obj:`eopf.product.EOProduct` with
    the parameter **store_or_path_url**, that can be a :obj:`str` or a :obj:`eopf.product.store.EOProductStore`.

    .. jupyter-execute::

        product_read_from_store = EOProduct("product_read", store_or_path_url=EOZarrStore(file_name))

    .. note::
        The default type when you provide a :obj:`str` is a :obj:`eopf.product.store.EOZarrStore`

    So now if you access to an elements of your product, it come from the zarr file.

    .. warning::
        You have to **open** your store before, using :obj:`eopf.product.EOProduct.open` or :obj:`eopf.product.conveniences.open_store`

    .. jupyter-execute::

        from eopf.product.conveniences import open_store

        with open_store(product_read_from_store, mode='r'):
            product_read_from_store["/measurements/geo_position/altitude"]

    .. jupyter-execute::

        with open_store(product_read_from_store, mode='r'):
            print(product_read_from_store["measurements/geo_position/altitude/cartesian"]._data)
            print(product_read_from_store["measurements/geo_position/altitude/polar"]._data.to_numpy())
            print(product_read_from_store["measurements/geo_position/longitude/cartesian"]._data)
            print(product_read_from_store["measurements/geo_position/longitude/polar"]._data.to_numpy())
            print(product_read_from_store["measurements/geo_position/latitude/cartesian"]._data)
            print(product_read_from_store["measurements/geo_position/latitude/polar"]._data.to_numpy())

    If you want to load a full product in memory, you can use the :obj:`eopf.product.EOProduct.load` method:

    .. jupyter-execute::

        with open_store(product_read_from_store):
            product_read_from_store.load()
        product_read_from_store["measurements/geo_position/latitude/polar"]


Writting Products
-----------------

    Writting is pretty similar, but you have to use the :obj:`eopf.product.EOProduct.write` method

    .. jupyter-execute::

        with product.open(mode="w", store_or_path_url=EOZarrStore(f"{output_folder}/{output_filename}")):
            product.write()

    .. warning::
        You have to **open** your store before, using :obj:`eopf.product.EOProduct.open` or :obj:`eopf.product.conveniences.open_store`
