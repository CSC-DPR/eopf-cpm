import asyncio
from abc import ABC, abstractmethod
from functools import wraps
from typing import Any, Callable, Coroutine, Optional

import click
import pkg_resources


class EOPFPluginCommandCLI(ABC, click.Command):
    """Abstract class used to extend eopf cli and provide other command

    Parameters
    ----------
    context_settings: dict, optional
        default values provide to click

    Attributes
    ----------
    name: str
        name of this command
    cli_params: Sequence[click.Parameter]
        all argument and option associated to this command
    help: str
        text use to specified to the user what this command is made for
    short_help: str
        shortter version of the help part
    epilog: str
        like help, but only provide at the end of the help command
    enable_help_option: bool
        indicate if the help option is provide automatically (default True)
    hidden: bool
        indicate if this command is hidden when it's search (default False)
    deprecated: bool
        indicate if this command is deprecated or not (default False)

    See Also
    --------
    click.Command
    """

    name: str
    cli_params: list[click.Parameter] = []
    help: str = ""
    short_help: str = ""
    epilog: str = ""
    enable_help_option = True
    hidden = False
    deprecated = False

    def __init__(
        self,
        context_settings: Optional[dict[str, Any]] = None,
        options_metavar: Optional[str] = "[OPTIONS]",
    ) -> None:
        super().__init__(
            self.name,
            context_settings=context_settings,
            callback=self.callback_function,
            params=self.cli_params,
            help=self.help,
            epilog=self.epilog,
            short_help=self.short_help,
            options_metavar=options_metavar,
            add_help_option=self.enable_help_option,
            no_args_is_help=self._no_args_is_help,
            hidden=self.hidden,
            deprecated=self.deprecated,
        )

    @staticmethod
    @abstractmethod
    def callback_function(*args: Any, **kwargs: Any) -> Any:
        """Abstract method to provide an interface for the logic to implement in this command"""

    @property
    def _no_args_is_help(self) -> bool:
        return any(isinstance(param, click.Argument) for param in self.cli_params)


class EOPFPluginGroupCLI(click.Group):
    """Abstract class used to extend eopf cli and provide other group of command

    Attributes:
    -----------
    name: str
        name of this group of command
    cli_commands: Sequence[click.Command]
        Sequence of command aggregate here

    Parameters
    ----------
    **attrs: Any
        any argument for click.Command, click.MultiCommand

    See Also
    --------
    click.Group
    """

    name: str
    cli_commands: list[click.Command] = []

    def __init__(self, **attrs: Any) -> None:
        super().__init__(self.name, self.cli_commands, **attrs)


class EOPFCLI(click.MultiCommand):
    """Command provide by the eopf cli and aggregate all sub command

    Sub Command are defined in the entry point 'eopf.cli' section.

    Examples
    --------
    [project.entry-points."eopf.cli"]
    my-cmd = "pkg.module.class_name"
    """

    def list_commands(self, ctx: click.Context) -> list[str]:
        return sorted(pkg_resources.get_entry_map("eopf").get("eopf.cli", {}).keys())

    def get_command(self, ctx: click.Context, cmd_name: str) -> Optional[click.Command]:
        cmd = pkg_resources.get_entry_map("eopf").get("eopf.cli", {})[cmd_name].load()
        return cmd()


@click.command(name="eopf", cls=EOPFCLI, add_help_option=True)
def eopf_cli() -> None:
    ...


def async_cmd(func: Callable[..., Any]) -> Callable[..., Coroutine[None, Any, Any]]:
    """Decorator to use click and async function / method"""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Coroutine[None, Any, Any]:
        return asyncio.run(func(*args, **kwargs))

    return wrapper


def click_callback(func: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap function to be call by click callback directly"""

    def wrapper(ctx: click.Context, param: click.Parameter, value: Any) -> Any:
        if value is not None:
            return func(value)

    return wrapper
