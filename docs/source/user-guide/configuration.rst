.. _configuration:

#############################
How to configure the EOPF CPM
#############################

You can use different way to configure the eopf:

    - using ini file with ``eopf`` section
    - using pyproject.toml with ``tool.eopf`` section
    - using environment variable setting the value in upper case
      and prefixing it by ``EOPF_``
      (example: ``configuration_folder`` => ``EOPF_CONFIGURATION_FOLDER``)

You can configure:

    - ``configuration_folder``: folder containing all modules configuration
      for ``qualitycontrol`` or ``logging`` for example.
      By default, ``configuration_folder`` is ``~/.eopf``.

Examples
--------

INI file
~~~~~~~~
For user that use ``INI`` file to configure tools and library

.. code-block:: ini

   [eopf]
   configuration-folder = /home/user/project/config

pyproject.toml file
~~~~~~~~~~~~~~~~~~~
For python developper that use ``pyproject.toml`` as project configuration

.. code-block:: toml

   [tool.eopf]
   configuration-folder = "/home/user/project/config"


Environment variable
~~~~~~~~~~~~~~~~~~~~
Easy way for all other users that are not familiar with ``INI`` file or ``pyproject.toml``

.. code-block:: bash

    EOPF_CONFIGURATION_FOLDER="./config" eopf trigger local trigger.json
