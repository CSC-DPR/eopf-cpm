What is triggering and how to use it
====================================

The triggering module is related to component that run (in one way or another) computing component.

the base class :py:class:`~eopf.triggering.abstract.EOTrigger` provide a method that parse a payload and execute
the processing unit from the payload.


Payload definition
------------------

the payload data is a dictionary that follow the following pattern::

    {
        "module": "",
        "processing_unit": "",
        "parameters": {
            "kwargs_name": ""
        },
        "I/O": {
            "modification_mode": "newproduct",
            "input_product": {
                "id": "OLCI",
                "path": "",
                "store_type": "safe"
            },
            "output_product": {
                "id": "output",
                "path": "output.zarr",
                "store_type": "zarr"
            }
        },
        "dask_context":{}
    }


and the component respect the following rules:

* **"module"**: string corresponding to the python path of the module (ex: "eopf.computing")
* **"processing_unit"**: EOProcessingUnit class name (ex: "SumProcessor")
* **"parameters"**: kwargs parameters to pass to the :py:meth:`~eopf.triggering.abstract.EOTrigger.run` method
* **"I/O"**: configuration for inputs and outputs

    - **"modification_mode"**: one of the following value:

        - **"newproduct"**: Create a new product on **output_product** config path ("w" file mode)
        - **"readonly"**: Read just the input without writting ("r" file mode)
        - **"inplace"**: Update the input ("r+" file mode)

    - **"input_product"** and **"output_product"** (only for "newproduct" mode): dictionary used to identify input (or output) product to use

        - **"id"**: name to give to :py:class:`~eopf.product.core.eo_product.EOProduct`
        - **"path"**: uri or path (relative to the runner) to the product (ex: "data/S3A_OL_1_EFR____NT_002.SEN3")
        - **"store_type"**: :py:class:`~eopf.product.store.store_factory.EOStoreFactory` identifier of the store for the given product

* **"dask_context"**: dictionary that contain one of those possible keys

    - **"local"**: define that dask is use in local config (oposed to "distributed"), followed by the scheduler type (ex: "local": "processes")
    - **"distributed"**: define that dask is use in distributed mode (oposed to "local"), followed by the scheduler name (ex: "distributed": "processes")


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
