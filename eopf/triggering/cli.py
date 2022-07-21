import json
import urllib.parse as urlparser
from typing import Any

import click
import requests

from eopf.cli import EOPFPluginCommandCLI, EOPFPluginGroupCLI, async_cmd, click_callback
from eopf.triggering.abstract import EOTrigger


@click_callback
def load_json_file(file_name: str) -> dict[str, Any]:
    """Wrap json load to automatically load from a filename in click.Command

    Parameters
    ----------
    file_name: str
        name of the file to load the json content

    Returns
    -------
    dict[str, Any]
    """
    with open(file_name) as f:
        data = json.load(f)
    for key in ["workflow", "breakpoints", "I/O", "dask_context"]:
        if isinstance(data.get(key), str):
            data[key] = load_json_file(data[key])
    return data


@click_callback
def format_server_info(value: Any) -> str:
    """Wrap urlparse to automatically format url with trigger endpoint

    Parameters
    ----------
    value: str
        url to the target web server

    Returns
    -------
    str
    """
    url = urlparser.urlparse(value)
    if not url.path.endswith("/run"):
        url = urlparser.urlparse(f"{url.geturl()}/run")
    return url.geturl()


class EOLocalCLITrigger(EOTrigger, EOPFPluginCommandCLI):
    """EOTrigger cli command to run locally an EOProcessingUnit from a specific
    json file.

    Parameters
    ----------
    context_settings: dict, optional
        default values provide to click

    See Also
    --------
    click.Command
    """

    name = "local"
    cli_params: list[click.Parameter] = [
        click.Argument(
            ["json-data-file"],
            type=click.Path(exists=True, file_okay=True, dir_okay=False),
            callback=load_json_file,
        ),
    ]
    help = "Trigger a specific EOProcessingUnit locally from the given json data"

    @staticmethod
    def callback_function(json_data_file: dict[str, Any]) -> None:  # type: ignore[override]
        """Run the EOTrigger.run with the json data

        Parameters
        ----------
        json_data_file: dict
            json data used as payload
        """
        click.echo(f"Run with {json_data_file}")
        EOLocalCLITrigger.run(json_data_file)


class EORequestCLITrigger(EOTrigger, EOPFPluginCommandCLI):
    """EOTrigger cli command to trigger web server from a json file

    Parameters
    ----------
    context_settings: dict, optional
        default values provide to click

    See Also
    --------
    click.Command
    """

    name = "request"
    cli_params: list[click.Parameter] = [
        click.Argument(
            ["json-data-file"],
            type=click.Path(exists=True, file_okay=True, dir_okay=False),
            callback=load_json_file,
        ),
        click.Option(
            ["--server-info"],
            default="http://127.0.0.1:8080",
            callback=format_server_info,
            help="target server (default: http://127.0.0.1:8080)",
        ),
    ]
    help = "Trigger a specific EOProcessingUnit on the target web server from the given json data"

    @staticmethod
    def callback_function(json_data_file: dict[str, Any], server_info: str) -> None:  # type: ignore[override]
        """Send the request to the web service to trigger EOTrigger.run*

        Parameters
        ----------
        json_data_file: dict
            json data used as payload
        server_info: str
            target server information, must start with scheme
        """
        r = requests.post(url=server_info, json=json_data_file)
        click.echo(f"Server return status code {r.status_code} with content: {r.content}")


class EOKafkaCLITrigger(EOTrigger, EOPFPluginCommandCLI):
    """EOTrigger cli command to send data from a json file to kafka server

    Parameters
    ----------
    context_settings: dict, optional
        default values provide to click

    See Also
    --------
    click.Command
    """

    name = "kafka"
    cli_params: list[click.Parameter] = [
        click.Argument(
            ["json-data-file"],
            type=click.Path(exists=True, file_okay=True, dir_okay=False),
            callback=load_json_file,
        ),
        click.Option(
            ["--kafka-server"],
            default="127.0.0.1:9092",
            help="Kafka server information (default 127.0.0.1:9092)",
        ),
        click.Option(["--kafka-topic"], default="run", help="Kafka topic (default 'run')"),
    ]
    help = "Trigger a specific EOProcessingUnit on the target kafka server from the given json data"

    @staticmethod
    @async_cmd
    async def callback_function(json_data_file: dict[str, Any], kafka_server: str, kafka_topic: str) -> None:
        """Send the request to kafka service to trigger EOTrigger.run

        Parameters
        ----------
        json_data_file: dict
            json data used as payload
        kafka_server: str
            target server information
        kafka_topic: str
            target topic
        """
        from aiokafka import AIOKafkaProducer

        producer = AIOKafkaProducer(bootstrap_servers=kafka_server)
        await producer.start()
        try:
            msg = await producer.send_and_wait(kafka_topic, json.dumps(json_data_file).encode())
        finally:
            await producer.stop()
        click.echo(f"{msg}")


class EOCLITrigger(EOTrigger, EOPFPluginGroupCLI):
    """EOTrigger cli command aggregator to trigger other services

    Parameters
    ----------
    **attrs: Any
        any argument for click.Command, click.MultiCommand

    See Also
    --------
    click.Group
    """

    name = "trigger"
    cli_commands: list[click.Command] = [
        EOLocalCLITrigger(),
        EORequestCLITrigger(),
        EOKafkaCLITrigger(),
    ]
    help = "CLI commands to trigger EOProcessingUnit"
