.. _triggering-usage:

What is triggering and how to use it
====================================

The triggering module is related to component that run (in one way or another) computing component.

the base class :py:class:`~eopf.triggering.abstract.EOTrigger` provide a method that parse a payload and execute
the processing unit from the payload.


Payload definition
------------------

the payload data is a dictionary that follow the following pattern::

    {
        "breakpoints": [
            {
                "related_unit": "preprocessing_unit",
                "break_mode": "s",
                "storage": "preprocessing_unit.zarr",
                "store_params": {}
            }
        ],
        "workflow": [
            {
                "module": "eopf.qualitycontrol.eo_qc_processor",
                "processing_unit": "EOQCProcessor",
                "name": "preprocessing_unit",
                "inputs": [
                    "OLCI"
                ],
                "parameters": {}
            }
        ],
        "I/O": {
            "modification_mode": "newproduct",
            "inputs_products": [
                {
                    "id": "OLCI",
                    "path": "data/S3B_OL_1_EFR____20220119T092920_20220119T093220_20220120T142503_0179_061_321_3240_LN1_O_NT_002.zarr",
                    "store_type": "zarr",
                    "store_params": {}
                }
            ],
            "output_product": {
                "id": "output",
                "path": "output.zarr",
                "store_type": "zarr",
                "store_params": {}
            }
        },
        "dask_context": {
            "cluster_type": "local",
            "cluster_config": {},
            "client_config": {}
        }
    }



and the component respect the following rules:

breakpoints:
    configure breakpoint component :ref:`breakpoint-usage`

        * **"related_unit"**: reference name of the processing unit concern by this breakpoint
        * **"break_mode"**: one of *r* (retrieve), *s* (skip), *w* (force write).
        * **"storage"**: uri to retrieve or write the breakpoint product
        * **"store_params"**: parameters to give to the :py:class:`EOZarrStore`

workflow:
    can be an item or a list of

        * **"name"**: identifier for the processing unit, can be use as `related_unit` in **"breakpoints"**
        * **"module"**: string corresponding to the python path of the module (ex: "eopf.computing")
        * **"processing_unit"**: EOProcessingUnit class name (ex: "SumProcessor")
        * **"inputs"**: list of product name or processing unit identifier use as inputs.
        * **"parameters"**: parameters to give to the processing unit at run time

I/O:
    configuration for inputs and outputs

        * **"modification_mode"**: one of the following value:

            - **"newproduct"**: Create a new product on **output_product** config path ("w" file mode)
            - **"readonly"**: Read just the input without writting ("r" file mode)
            - **"inplace"**: Update the input ("r+" file mode)

        * **"input_product"** and **"output_product"** (only for "newproduct" mode): dictionary used to identify input (or output) product to use

            - **"id"**: name to give to :py:class:`~eopf.product.core.eo_product.EOProduct`
            - **"path"**: uri or path (relative to the runner) to the product (ex: "data/S3A_OL_1_EFR____NT_002.SEN3")
            - **"store_type"**: :py:class:`~eopf.product.store.store_factory.EOStoreFactory` identifier of the store for the given product

dask_context
    configuration for :py:class:`~eopf.triggering.conf.dask_configuration.DaskContext`

        * **"cluster_type"**: type of dask cluster the should be used
        * **"cluster_config"**: configuration to five to the dask cluster
        * **"client_config"**: configuration for the :py:class:`~dask.distributed.Client`


CLI triggers
------------

Multiple Trigger command are provide by the eopf-cpm package inside the **eopf** command, for example

.. code-block:: bash

    $ eopf trigger

will show you all available command to trigger processing unit from an input json file.

Available commands are:

* **eopf kafka-consumer**: run an asynchronous message consumer from the specified kafka server and topic
* **eopf web-server**: run an asynchronous web server exposed to the specified host and port
* **eopf trigger**: load a json file as payload following the previous template

    - **kafka**: send a message with the given payload json file to the specified kafka server and topic
    - **request**: send a post request with the given payload to the given web server
    - **local**: run :py:meth:`~eopf.triggering.abstract.EOTrigger.run` with the given payload json file data
