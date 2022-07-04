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
                try:
                    EOEventTrigger.run(json.loads(msg.value))
                except Exception as e:
                    logger.exception(e)
        finally:
            await consumer.stop()
