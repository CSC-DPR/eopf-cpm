import json

import click
from aiokafka import AIOKafkaConsumer

from eopf.cli import EOPFPluginCommandCLI, async_cmd
from eopf.logging import logger
from eopf.triggering.abstract import EOTrigger


class EOEventTrigger(EOTrigger, EOPFPluginCommandCLI):
    name = "kafka-consumer"
    cli_params: list[click.Parameter] = [
        click.Option(["--kafka-server"], default="127.0.0.1:9092"),
        click.Option(["--kafka-topic"], default="run"),
    ]

    @staticmethod
    @async_cmd
    async def callback_function(kafka_server: str, kafka_topic: str) -> None:
        consumer = AIOKafkaConsumer(kafka_topic, bootstrap_servers=kafka_server)
        await consumer.start()
        try:
            async for msg in consumer:
                logger.info(f"Consume message {msg} for {kafka_server}/{kafka_topic}")
                EOEventTrigger.run(json.loads(msg.value))
        finally:
            await consumer.stop()
