Logging
=======

We provide the developpers of eopf-cpm the ability to dinamically define and
map logging configurations through the :py:class:`~eopf.logging.log.EOLogFactory`. Thus, the developper can easily
create loggers with particular options. Out of the box we provide the developpers with 2 logging configurations *default* and
*file*. Nevertheless, the developpers can provide their own configurations. :py:class:`~eopf.logging.log.EOLogFactory` is available in the
:py:mod:`eopf.logging` module


.. jupyter-execute::
    :hide-output:

    from eopf.logging import EOLogFactory

Restrictions on the logging configurations
------------------------------------------

Generally speaking we do not impose many restrictions. However, the
following should be followed for the sake of uniformity:

- The format of the messages written in log files should as given below:

    .. code-block:: python

        format = "%(asctime)s : %(levelname)s : %(module)s : %(funcName)s : %(lineno)d : (Process Details : (%(process)d, %(processName)s), Thread Details : (%(thread)d, %(threadName)s))\nLog : %(message)s"
        datefmt = "%Y-%m-%d %H:%M:%S"


- For log files one should use :py:class:`~logging.handlers.TimedRotatingFileHandler`, with a rotation *interval* and *backupcount* which can be freely defined.  Neverthless, we recommend either a daily or weekly rotation.

Out of the box logging configurations
-------------------------------------

We advise not to use the *default* configuration as provided by
eopf-cpm as it just displays the log to *stdout*; it is meant only
for developping purposes. Nevertheless, you are free to modify and
extend it as you please

The *file* configuration can be used both for production and
developping purposes.


Creating loggers with EOLogFactory
----------------------------------

Loggers can be created by either providing the *name* of the
logger and/or configuration name (*cfg_name*). If they are
omitted the *default* value will be used.

    .. code-block:: python

        logger = EOLogFactory().get_log()


    .. code-block:: python

        logger = EOLogFactory().get_log(name="FirstLogger")


    .. code-block:: python

        logger = EOLogFactory().get_log(name="SecondLogger", cfg_name='file')


Using loggers created with EOLogFactory
---------------------------------------

Since :py:class:`~eopf.logging.log.EOLogFactory` returns standard python loggers, you can use these
loggers as you any other python logger, as exemplified below.

    .. code-block:: python

        logger.info("a random message")

        a_parameter = "text of the parameter"
        logger.error(f"a random message with a parameter: {a_parameter}")

        try:
            float("xxx")
        except Exception as e:
            log.exception(f"Exception {e} encountered when converting to float")

Loading other log configurations
--------------------------------

One type of logging configuration files is supported: **.json**.
The default logging configuration directory in located at **eopf/logging/conf/**. We
recommend that any other log configurations to be added here.
Keep in mind that the :py:class:`~eopf.logging.log.EOLogFactory` is just a mapper of file configurations, it will
not store the actual files in memory.

One can see the mapped logging configurations by looking in the
*_cfgs* attribute of the :py:class:`~eopf.logging.log.EOLogFactory`.


    .. jupyter-execute::

        EOLogFactory()._cfgs


There are two options available for mapping new logging configuration:

    - map one configuration, with **register_cfg()**

        .. code-block:: python

            EOLogFactory.register_cfg("log_cfg_name", "log_cfg_file_path.json")

    - map all configurations from a dir and remove current ones, with **set_cfg_dir()**

        .. code-block:: python

            EOLogFactory.set_cfg_dir("log_cfgs_dir_path")


Guarding for unnecesary object creation
---------------------------------------

:py:class:`~eopf.logging.log.EOLogFactory` is a singletone, no matter how many times one instantiates it you will get the same object.

    .. jupyter-execute::

        lf1 = EOLogFactory()
        lf2 = EOLogFactory()
        print(id(lf1))
        print(id(lf2))


Python Loggers are indexed by their name, so if you need a logger multiple times just
request the same name. This helps avoid creating many objects of the same type.

    .. jupyter-execute::

        log1 = EOLogFactory().get_log(cfg_name="default", name="root")
        log2 = EOLogFactory().get_log(cfg_name="default", name="root")
        print(id(log1))
        print(id(log2))


Unwanted practice
-----------------

Avoind using different names for the same log configuration. We recommend using one
log name per configuration, otherwise you would create unnecesary loggers; as
depicted below.


    .. jupyter-execute::

        log1 = EOLogFactory().get_log(cfg_name="console", name="same name")
        log2 = EOLogFactory().get_log(cfg_name="console", name="different name")
        print(id(log1))
        print(id(log2))


Dask logging
------------

We recommend that you use the dask.yaml logging configuration file, placed in
*eopf/logging/conf/dask.yaml*. To use this logging configuration copy it to
*~/.config/dask*, or where your dask configuration files reside.
