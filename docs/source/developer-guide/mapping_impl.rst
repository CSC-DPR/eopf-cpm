How to write a safe mapping
===========================

The safe store use json mapping to match for each legacy product the target_path EOPath given to
the SAFE store to it's actual location (sub file source_path + local_path in the file).


An example of mapping::

    {
        "recognition": {
            "filename_pattern": "S2._MSIL1C_.*SAFE",
        },
        "data_mapping": [
            {
                "source_path": "GRANULE/[^/]*/AUX_DATA/AUX_ECMWFT:tcwv",
                "target_path": "/conditions/meteo/tcwv",
                "item_format": "grib",
                "parameters": {
                    "attributes": {
                        "long_name": "total column water vapour"
                    },
                    "dimensions": [ "latitude_meteo", "longitude_meteo" ]
                }
            },
            {
                "source_path": "GRANULE/[^/]*/IMG_DATA/.*B02.jp2",
                "target_path": "/coordinates/image_grid_10m/y_10m",
                "item_format": "jp2",
                "local_path": "n1:Geometric_Info/Tile_Geocoding/Geoposition[@resolution='10']/ULY",
                "parameters": {
                    "dimensions": [ "y_10m" ],
                    "attributes": {
                        "standard_name": "longitude",
                        "units": "degrees_east"
                    },
                    "sub_array" : {
                        "dim_0": 0
                    },
                    "pack_bits": 0
                },
                "accessor_config": {
                    "namespace": "xml_mapping/namespace",
                    "step_path": "xml_mapping/xmltp/step_y"
                },
            }
        ],
        "xml_mapping": {
            "namespace":{
                "n1": "https://psd-14.sentinel2.eo.esa.int/PSD/S2_PDI_Level-1C_Tile_Metadata.xsd"
            },
            "xmltp": {
                "step_x": "n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/COL_STEP",
                "step_y": "n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/ROW_STEP",
                "values": "n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/Values_List/VALUES"
            }
        }
    }

recognition
-----------

The recognition dictionary contain information for recognizing that a safe folder is read with this json mapping.

filename_pattern :
    A regex pattern that corresponding safe data folder match.

data mapping
------------

The data mapping list contain all the mappings each from a target_path EOPath to a local path inside one of the SAFE files.

accessor_config :
    dictionary of argv key to path in the json of the argv value given to the accessor when it's opened. In the example the passed argv would be:

    .. code-block:: python

        argv = {
            "namespace": {
                "n1": "https://psd-14.sentinel2.eo.esa.int/PSD/S2_PDI_Level-1C_Tile_Metadata.xsd"
            },
            "step_path": "n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/ROW_STEP"
        }

item_format :
    registration string of an accessor/store in the EOStoreFactory. We currently provide : grib, jp2, netcdf,
    netcdf_string_to_time, xmlangles, xmlmetadata, xmltp, zarr

is_optional:
    (optional) a boolean indicating if the presence of this data is optional (default to false)

local_path :
    (optional) an alternative way of passing the local_path used by the accessor to get the item.
    It's not recommended to use both local_path and source_path:local_path.

parameters :
    (all optional) the main way to apply simple modifications to object returned by accessor before giving them to the product

    * attributes : Add the attributes in the passed dictionary
    * dimensions : Change dimensions names to the passed list. Applied after pack_bits/sub_array. Must have the same dimension count as the value after all the others transformations.
    * pack_bits : Pack the bits in the passed dimension. The dimension is removed. Aka replace a dimension of bit by an integer.
    * sub_array : Index the key dimensions in the passed dictionary with the dictionary values indexes. If this result in a size 1 dimension it is removed. In the example it's used to remove a dims by only taking it's first line.

source_path :
    A file_regex_pattern or file_regex_pattern:local_path string with:

    * file_regex_patern : regex pattern of the safe file local path (to the safe root) corresponding to this mapping. Can't contain ":" character.
    * local_path : (optional) local path used by the accessor to get the item.

target_path :
    EOPath corresponding to this data mapping

other
-----

The rest of the json file can contain anything. It can notably be used by the `accessor_config` field in a data_mapping that pass part of the json file when the accessor is openened.

In the example xml_mapping is used like that.

Adding mapping in a plugin
--------------------------

To add new mapping in a ``eopf-cpm`` plugin, you can reference a new *entry points* section named **eopf.store.mapping_folder**
in my **setup.cfg** or **pyproject.toml**,

* pyproject.toml **flit** format:
    .. code-block:: toml

        [project.entry-points."store.mapping_folder"]
        newfoldername = path.to.my.mapping.folder

* setup.cfg **setuptools** format:
    .. code-block:: toml

        [options.entry_points]
        eopf.store.mapping_folder =
            newfoldername = path.to.my.mapping.folder

or if you use it as a standalone you can create and add your own mapping in your configuration folder
(see :ref:`configuration`)

Use your store (without modifying the eopf sources)
---------------------------------------------------
Create a EOMappingFactory, register your mapping to it and initialise your SafeStore with it as *mapping_factory*.

.. note:: You can also provide custom accessors or parameter transformations to the SAFE Store with *store_factory* and *parameters_transformations*
