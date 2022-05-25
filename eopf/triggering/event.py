import json
import logging

import click
from aiokafka import AIOKafkaConsumer

from eopf.cli import EOPFPluginCommandCLI, async_cmd
from eopf.triggering.abstract import EOTrigger

logger = logging.getLogger("eopf")


class EOEventTrigger(EOTrigger, EOPFPluginCommandCLI):
    """EOTrigger cli command to run a kafka message consumer

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

    name = "kafka-consumer"
    cli_params: list[click.Parameter] = [
        click.Option(
            ["--kafka-server"],
            default="127.0.0.1:9092",
            help="Kafka server information (default 127.0.0.1:9092)",
        ),
        click.Option(["--kafka-topic"], default="run", help="Kafka topic (default 'run')"),
    ]
    help = "Get and load messages from kafka an execute EOTrigger"

    @staticmethod
    @async_cmd
    async def callback_function(kafka_server: str, kafka_topic: str) -> None:
        """Run the EOtrigger.run for each msg find in the topic"""
        consumer = AIOKafkaConsumer(kafka_topic, bootstrap_servers=kafka_server)
        await consumer.start()
        try:
            async for msg in consumer:
                logger.info(f"Consume message {msg} for {kafka_server}/{kafka_topic}")
                EOEventTrigger.run(json.loads(msg.value))
        finally:
            await consumer.stop()
