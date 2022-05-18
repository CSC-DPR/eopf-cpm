How to provide new cli command
==============================

EOPF-CPM command is based on `click`_ and provide a simple way to extend it.

Plugins Command or Group
------------------------

the **eopf.cli** provide two classes that help to write cli commands for eopf.

* :obj:`eopf.cli.EOPFPluginCommandCLI`: that provide an interface to write subcommand
* :obj:`eopf.cli.EOPFPluginGroupCLI`: with an interface for command group

You only need to write an inherited class of one of them and then, in the *entry point* **eopf.cli**
you have to declare it.

Writing Command
~~~~~~~~~~~~~~~

We need to add this new command:

.. code-block:: python

    class MyNewCliCommand(EOPFPluginCommandCLI):
        name = "mynewclicommand"
        cli_params = [
            click.Option(["--my-opt"], default=False)
            click.Argument(["my-arg"])
        ]
        help = "provide simple cli to something"

        @staticmethod
        def callback_function(my_opt, my_arg):
            dosomething(my_opt, my_arg)

in my **setup.cfg** or **pyproject.toml**,
I refer to this class in the specific *entry point* secion **eopf.cli**:

* pyproject.toml **flit** format:
    .. code-block:: toml

        [project.entry-points."eopf.cli"]
        mynewclicommand = path.to.my.cli:MyNewCliCommand

* setup.cfg **setuptools** format:
    .. code-block:: toml

        [options.entry_points]
        eopf.cli =
            mynewclicommand = path.to.my.cli.MyNewCliCommand

or with a EOPFPluginGroupCLI:

.. code-block:: python

    class MyNewCliGroupCommand(EOPFPluginGroupCLI):
        name = "mynewcligroup"
        cli_commands = [
            MyNewCliCommand()
        ]
        help = "provide simple cli group with sub command"

in my **setup.cfg** or **pyproject.toml**, i refer to this class in the specific *entry point* secion **eopf.cli**::

    [project.entry-points."eopf.cli"]
    mynewcligroup = path.to.my.cli.MyNewCliGroupCommand



.. _click: https://click.palletsprojects.com/
