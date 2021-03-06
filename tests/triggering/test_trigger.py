import datetime
import json
import re
import time
from multiprocessing import Process
from unittest import mock

import pytest
from click.testing import CliRunner
from fastapi.testclient import TestClient

from eopf.product.conveniences import init_product
from eopf.product.store.store_factory import EOStoreFactory
from eopf.triggering import EOCLITrigger, EOEventTrigger, EOWebTrigger
from eopf.triggering.abstract import EOTrigger
from eopf.triggering.conf.dask_configuration import DaskContext
from eopf.triggering.parser.workflow import EOProcessorWorkFlow


@pytest.fixture
def fake_kafka_consumer(TRIGGER_JSON_FILE):
    with open(TRIGGER_JSON_FILE) as f:
        data = json.load(f)

    class FakeKafkaConsumer:
        def __init__(self, length=1):
            self.length = length
            self._called = 0

        def __aiter__(self):
            if self._called >= self.length:
                raise StopAsyncIteration
            return self

        def get_value(self):
            return data

        async def __anext__(self):
            if self._called >= self.length:
                raise StopAsyncIteration
            from aiokafka.structs import ConsumerRecord

            self._called += 1
            return ConsumerRecord(
                "run",
                partition=0,
                offset=0,
                timestamp=datetime.datetime.now().timestamp(),
                timestamp_type=0,
                key="a",
                value=json.dumps(self.get_value()),
                checksum=1,
                serialized_key_size=1,
                serialized_value_size=1,
                headers=[],
            )

    return FakeKafkaConsumer(1)


@pytest.fixture
def server():
    runner = CliRunner()
    proc = Process(target=runner.invoke, args=(EOWebTrigger(),), daemon=True)
    proc.start()
    # Wait 1 sec that the server is up
    time.sleep(1)
    yield
    proc.terminate()


@pytest.fixture
def client():
    return TestClient(EOWebTrigger())


@pytest.mark.unit
@pytest.mark.parametrize(
    "args, match_output",
    [(["request"], r"Server return status code [0-9]{3} with content: .*"), (["local"], r"Run with .*")],
)
def test_cli_trigger(server, TRIGGER_JSON_FILE, args, match_output):
    runner = CliRunner()
    with mock.patch("tests.computing.test_abstract.TestAbstractProcessor.run", return_value=init_product("")):
        r = runner.invoke(EOCLITrigger(), args=" ".join([*args, TRIGGER_JSON_FILE]))
    assert re.match(match_output, r.output) is not None, r.output
    assert r.exception is None
    assert r.exit_code == 0


# TODO: To Update when a kafka can be accessible in the ci
@pytest.mark.unit
def test_kafka_send(TRIGGER_JSON_FILE):
    runner = CliRunner()
    with (
        mock.patch("aiokafka.AIOKafkaProducer.start") as start,
        mock.patch("aiokafka.AIOKafkaProducer.send_and_wait") as send,
        mock.patch("aiokafka.AIOKafkaProducer.stop") as stop,
        mock.patch("tests.computing.test_abstract.TestAbstractProcessor.run", return_value=init_product("")),
    ):
        runner.invoke(EOCLITrigger(), args=f"kafka {TRIGGER_JSON_FILE}")
    assert start.call_count == 1
    assert send.call_count == 1
    assert stop.call_count == 1


# TODO: To Update when a kafka can be accessible in the ci
@pytest.mark.unit
def test_kafka_consume(fake_kafka_consumer):
    runner = CliRunner()
    with (
        mock.patch("aiokafka.AIOKafkaConsumer.start") as start,
        mock.patch("aiokafka.AIOKafkaConsumer.__aiter__", return_value=fake_kafka_consumer) as retrieve,
        mock.patch("aiokafka.AIOKafkaConsumer.stop") as stop,
        mock.patch("aiokafka.AIOKafkaConsumer._closed", return_value=True),
        mock.patch("tests.computing.test_abstract.TestAbstractProcessor.run", return_value=init_product("")),
    ):
        r = runner.invoke(EOEventTrigger(), args=())
        assert r.exception is None
        assert r.exit_code == 0

    assert start.call_count == 1
    assert retrieve.call_count == 1
    assert stop.call_count == 1


@pytest.mark.unit
def test_web_trigger(client, TRIGGER_JSON_FILE):
    with open(TRIGGER_JSON_FILE) as f:
        data = json.load(f)
    with mock.patch("tests.computing.test_abstract.TestAbstractProcessor.run", return_value=init_product("")):
        r = client.post("/run", json=data)
        assert r.status_code == 200
        assert r.json() == {}

        r = client.post("/run", json={})
        assert r.status_code == 200
        assert "err" in r.json()


@pytest.mark.unit
@pytest.mark.parametrize("trigger_class", [EOTrigger, EOCLITrigger, EOEventTrigger, EOWebTrigger])
@pytest.mark.parametrize(
    "payload",
    [
        {
            "workflow": {
                "module": "tests.computing.test_abstract",
                "processing_unit": "TestAbstractProcessor",
                "parameters": {"key": "value"},
            },
            "I/O": {
                "modification_mode": "newproduct",
                "inputs_products": [{"id": "OLCI", "path": "product_path", "store_type": "safe", "store_params": {}}],
                "output_product": {"id": "output", "path": "output.zarr", "store_type": "zarr", "store_params": {}},
            },
        },
        {
            "workflow": {
                "module": "tests.computing.test_abstract",
                "processing_unit": "TestAbstractProcessor",
                "parameters": {"key": "value"},
            },
            "I/O": {
                "modification_mode": "newproduct",
                "inputs_products": [{"id": "OLCI", "path": "product_path", "store_type": "safe", "store_params": {}}],
                "output_product": {"id": "output", "path": "output.zarr", "store_type": "zarr", "store_params": {}},
            },
        },
        {
            "workflow": [
                {
                    "module": "tests.computing.test_abstract",
                    "processing_unit": "TestAbstractProcessor",
                    "parameters": {"key": "value"},
                    "name": "unit_1",
                    "inputs": ["OLCI"],
                },
                {
                    "module": "tests.computing.test_abstract",
                    "processing_unit": "TestAbstractProcessor",
                    "parameters": {"key": "value"},
                    "inputs": ["unit_1"],
                    "name": "unit_1",
                },
            ],
            "I/O": {
                "modification_mode": "newproduct",
                "inputs_products": [{"id": "OLCI", "path": "product_path", "store_type": "safe", "store_params": {}}],
                "output_product": {"id": "output", "path": "output.zarr", "store_type": "zarr", "store_params": {}},
            },
        },
    ],
)
def test_extract_payload(trigger_class, payload):
    (
        processing_unit,
        io_config,
        ctx,
    ) = trigger_class.extract_from_payload(payload)
    factory = EOStoreFactory()
    if isinstance(processing_unit, EOProcessorWorkFlow):
        units_classes, parameters = zip(
            *[(unit["processing_unit"], unit.get("parameters", {})) for unit in payload["workflow"]]
        )
        assert all(
            (unit.processing_unit.__class__.__name__ in units_classes and unit.parameters in parameters)
            for unit in processing_unit.workflow
        )
    else:
        assert processing_unit.processing_unit.__class__.__name__ == payload["workflow"].get("processing_unit")
        assert processing_unit.parameters == payload["workflow"].get("parameters", {})
    inputs_products_data = payload["I/O"].get("inputs_products")
    inputs_products = io_config["inputs"]
    for product, input_product_data in zip(inputs_products, inputs_products_data):
        input_product = product["instance"]
        assert input_product.name == input_product_data.get("id")
        assert type(input_product.store) == factory.item_formats[input_product_data.get("store_type")]
        assert input_product.store.url == input_product_data.get("path")
        assert product["parameters"] == input_product_data.get("store_params", {})

    output_product_data = payload["I/O"].get("output_product")
    output_store = io_config["output"]["instance"]
    assert type(output_store) == factory.item_formats[output_product_data.get("store_type")]
    assert output_store.url == output_product_data.get("path")
    assert io_config["output"]["parameters"] == payload["I/O"].get("output_product", {}).get("store_params", {})

    assert isinstance(ctx, DaskContext)


@pytest.mark.unit
@pytest.mark.parametrize("trigger_class", [EOTrigger, EOCLITrigger, EOEventTrigger, EOWebTrigger])
@pytest.mark.parametrize(
    "payload, exception",
    [
        (
            {
                "workflow": {
                    "module": "tests.computing.test_abstract",
                    "processing_unit": "",
                },
                "I/O": {
                    "modification_mode": "newproduct",
                    "inputs_products": [{"id": "OLCI", "path": "product_path", "store_type": "safe"}],
                    "output_product": {"id": "output", "path": "output.zarr", "store_type": "zarr"},
                },
            },
            AttributeError,
        ),
        (
            {
                "workflow": {
                    "module": "",
                    "processing_unit": "TestAbstractProcessor",
                },
                "I/O": {
                    "modification_mode": "newproduct",
                    "inputs_products": [{"id": "OLCI", "path": "product_path", "store_type": "safe"}],
                    "output_product": {"id": "output", "path": "output.zarr", "store_type": "zarr"},
                },
            },
            ValueError,
        ),
        (
            {
                "workflow": {
                    "module": "aaaa",
                    "processing_unit": "TestAbstractProcessor",
                },
                "I/O": {
                    "modification_mode": "newproduct",
                    "inputs_products": [{"id": "OLCI", "path": "product_path", "store_type": "safe"}],
                    "output_product": {"id": "output", "path": "output.zarr", "store_type": "zarr"},
                },
            },
            ModuleNotFoundError,
        ),
        (
            {
                "workflow": {
                    "module": "tests.computing.test_abstract",
                    "processing_unit": "TestAbstractProcessor",
                    "parameters": {"key": "value"},
                },
                "I/O": {
                    "modification_mode": "invalide_mode",
                    "inputs_products": [{"id": "OLCI", "path": "product_path", "store_type": "safe"}],
                    "output_product": {"id": "output", "path": "output.zarr", "store_type": "zarr"},
                },
            },
            ValueError,
        ),
    ],
)
def test_failed_extract_payload(trigger_class, payload, exception):
    with pytest.raises(exception):
        trigger_class.extract_from_payload(payload)
