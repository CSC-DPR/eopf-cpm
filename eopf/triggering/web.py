import logging

import click
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.types import Receive, Scope, Send

from eopf.cli import EOPFPluginCommandCLI
from eopf.triggering.abstract import EOTrigger

logger = logging.getLogger("eopf")


class EOWebTrigger(EOTrigger, FastAPI, EOPFPluginCommandCLI):
    """EOTrigger cli command to run a web server

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

    name = "web-server"
    cli_params: list[click.Parameter] = [
        click.Option(["--host"], default="127.0.0.1", help="host information (default 127.0.0.1)"),
        click.Option(["--port"], default=8080, help="Port (default 8080)"),
        click.Option(["--log-level"], default="info"),
    ]
    help = "Run web server to run EOTrigger with post payload"

    def __init__(self) -> None:
        FastAPI.__init__(self)
        EOPFPluginCommandCLI.__init__(self)
        EOTrigger.__init__(self)
        self.add_api_route("/run", self.run_request, methods=["POST"])

    @staticmethod
    async def run_request(request: Request) -> JSONResponse:
        """API route provide a simple way to execute EOTrigger.run

        Parameters
        ----------
        request: Request
            post information (should be json data)

        Returns
        -------
        JSONResponse
            if "err" is provide, an error as occur
        """
        try:
            payload = await request.json()
            logger.info(f"Triggered with {payload}")
            EOWebTrigger.run(payload)
        except Exception as e:
            logger.exception(e)
            import sys
            import traceback

            *_, exc_traceback = sys.exc_info()
            return JSONResponse(content={"err": "\n".join(traceback.format_tb(exc_traceback))}, status_code=200)
        return JSONResponse(content={}, status_code=200)

    @staticmethod
    def callback_function(host: str, port: int, log_level: str) -> None:  # type: ignore[override]
        """Start a web server with the given information

        Parameters
        ----------
        host: str
            On which IP/host name should start
        port: int
            On which port start
        log_level: str
            base level information
        """
        uvicorn.run(EOWebTrigger(), host=host, port=port, log_level=log_level)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:  # type: ignore[override]
        await FastAPI.__call__(self, scope, receive, send)
