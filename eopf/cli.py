import asyncio
from abc import ABC, abstractmethod
from functools import wraps
from typing import Any, Callable, Coroutine, Optional

import click
import pkg_resources


class EOPFPluginCommandCLI(ABC, click.Command):

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
        ...

    @property
    def _no_args_is_help(self) -> bool:
        return any(isinstance(param, click.Argument) for param in self.cli_params)


class EOPFPluginGroupCLI(click.Group):
    name: str
    cli_commands: list[click.Command] = []

    def __init__(self, **attrs: Any) -> None:
        super().__init__(self.name, self.cli_commands, **attrs)


class EOPFCLI(click.MultiCommand):
    def list_commands(self, ctx: click.Context) -> list[str]:
        return sorted(pkg_resources.get_entry_map("eopf").get("eopf.cli", {}).keys())

    def get_command(self, ctx: click.Context, cmd_name: str) -> Optional[click.Command]:
        cmd = pkg_resources.get_entry_map("eopf").get("eopf.cli", {})[cmd_name].load()
        return cmd()


@click.command(name="eopf", cls=EOPFCLI)
def eopf_cli() -> None:
    ...


def async_cmd(func: Callable[..., Any]) -> Callable[..., Coroutine[None, Any, Any]]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Coroutine[None, Any, Any]:
        return asyncio.run(func(*args, **kwargs))

    return wrapper
