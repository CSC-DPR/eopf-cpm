import json
import urllib.parse as urlparser
from typing import Any, Callable

import click
import requests

from eopf.cli import EOPFPluginCommandCLI, EOPFPluginGroupCLI, async_cmd
from eopf.triggering.abstract import EOTrigger


def click_callback(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(ctx: click.Context, param: click.Parameter, value: Any) -> Any:
        if value is not None:
            return func(value)

    return wrapper


@click_callback
def load_json_file(file_name: str) -> dict[str, Any]:
    with open(file_name) as f:
        return json.load(f)


@click_callback
def format_server_info(value: Any) -> str:
    url = urlparser.urlparse(value)
    if not url.path.endswith("/run"):
        url = urlparser.urlparse(f"{url.geturl()}/run")
    return url.geturl()


class EOLocalCLITrigger(EOTrigger, EOPFPluginCommandCLI):
    name = "local"
    cli_params: list[click.Parameter] = [
        click.Argument(
            ["json-data-file"],
            type=click.Path(exists=True, file_okay=True, dir_okay=False),
            callback=load_json_file,
        ),
    ]

    @staticmethod
    def callback_function(json_data_file: dict[str, Any]) -> None:  # type: ignore[override]
        click.echo(f"Run with {json_data_file}")
        EOLocalCLITrigger.run(json_data_file)


class EORequestCLITrigger(EOTrigger, EOPFPluginCommandCLI):
    name = "request"
    cli_params: list[click.Parameter] = [
        click.Argument(
            ["json-data-file"],
            type=click.Path(exists=True, file_okay=True, dir_okay=False),
            callback=load_json_file,
        ),
        click.Option(["--server-info"], default="http://127.0.0.1:8080", callback=format_server_info),
    ]

    @staticmethod
    def callback_function(json_data_file: dict[str, Any], server_info: str) -> None:  # type: ignore[override]
        r = requests.post(url=server_info, json=json_data_file)
        click.echo(f"Server return status code {r.status_code} with content: {r.content}")


class EOKafkaCLITrigger(EOTrigger, EOPFPluginCommandCLI):
    name = "kafka"
    cli_params: list[click.Parameter] = [
        click.Argument(
            ["json-data-file"],
            type=click.Path(exists=True, file_okay=True, dir_okay=False),
            callback=load_json_file,
        ),
        click.Option(["--kafka-server"], default="127.0.0.1:9092"),
        click.Option(["--kafka-topic"], default="run"),
    ]

    @staticmethod
    @async_cmd
    async def callback_function(json_data_file: dict[str, Any], kafka_server: str, kafka_topic: str) -> None:
        from aiokafka import AIOKafkaProducer

        producer = AIOKafkaProducer(bootstrap_servers=kafka_server)
        await producer.start()
        try:
            msg = await producer.send_and_wait(kafka_topic, json.dumps(json_data_file).encode())
        finally:
            await producer.stop()
        click.echo(f"{msg}")


class EOCLITrigger(EOTrigger, EOPFPluginGroupCLI):
    name = "trigger"
    cli_commands: list[click.Command] = [
        EOLocalCLITrigger(),
        EORequestCLITrigger(),
        EOKafkaCLITrigger(),
    ]
