import click
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.types import Receive, Scope, Send

from eopf.cli import EOPFPluginCommandCLI
from eopf.logging import logger
from eopf.triggering.abstract import EOTrigger


class EOWebTrigger(EOTrigger, FastAPI, EOPFPluginCommandCLI):
    """
    Expected behaviour : WebServiceTrigger is a permanent service which exposes the following endpoints:
     * run : run the asked service with the given payload as a json stream
     * healthcheck : check service is alive and well
    """

    name = "web-server"
    cli_params: list[click.Parameter] = [
        click.Option(["--host"], default="127.0.0.1"),
        click.Option(["--port"], default=8080),
        click.Option(["--log-level"], default="info"),
    ]

    def __init__(self) -> None:
        FastAPI.__init__(self)
        EOPFPluginCommandCLI.__init__(self)
        EOTrigger.__init__(self)
        self.add_api_route("/run", self.run_request, methods=["POST"])

    @staticmethod
    async def run_request(request: Request) -> JSONResponse:
        try:
            payload = await request.json()
            logger.info(f"Triggered with {payload}")
            EOWebTrigger.run(payload)
        except Exception as e:
            logger.error(f"An error occur: {e}")
            return JSONResponse(content={"err": str(e)}, status_code=200)
        return JSONResponse(content={}, status_code=200)

    @staticmethod
    def callback_function(host: str, port: int, log_level: str) -> None:  # type: ignore[override]
        uvicorn.run(EOWebTrigger(), host=host, port=port, log_level=log_level)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:  # type: ignore[override]
        await FastAPI.__call__(self, scope, receive, send)
