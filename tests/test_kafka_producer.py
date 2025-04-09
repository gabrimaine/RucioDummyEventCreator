import unittest
import json
import uuid
import lsst.utils.tests
from unittest.mock import MagicMock, patch
from lsst.rucioevents.kafka_producer import RucioKafkaProducer


class TestRucioKafkaProducer(unittest.TestCase):
    def setUp(self):
        """Setup the config."""
        self.mock_producer = MagicMock()
        self.kafka_producer = RucioKafkaProducer()
        self.kafka_producer.producer = self.mock_producer

    def test_send_event_success(self):
        """Check event creation."""
        event = {"key": "test_key", "data": "test_data"}
        topic = "Topic"
        self.kafka_producer.send_event(topic, [event])
        self.mock_producer.produce.assert_called_once_with(
            topic=topic,
            key=event["key"],
            value=json.dumps(event),
            callback=unittest.mock.ANY,  # I don't know how to test directly the callback
        )
        self.mock_producer.flush.assert_called_once()

    def test_send_event_no_key(self):
        """Test no key event."""
        event = {"data": "test_data"}
        topic = "Topic"
        self.kafka_producer.send_event(topic, [event])
        self.mock_producer.produce.assert_called_once()
        args, kwargs = self.mock_producer.produce.call_args
        key = kwargs["key"]
        if isinstance(key, bytes):
            key = key.decode("utf-8")
        if key.startswith("b'") and key.endswith("'"):
            key = key[2:-1]
        self.assertIsInstance(uuid.UUID(key), uuid.UUID)
        self.assertEqual(kwargs["value"], json.dumps(event))
        self.mock_producer.flush.assert_called_once()

    def test_send_event_multiple(self):
        """Chem many events production."""
        events = [
            {"key": "key1", "data": "data1"},
            {"key": "key2", "data": "data2"},
        ]
        self.kafka_producer.send_event("topic", events)
        self.assertEqual(self.mock_producer.produce.call_count, 2)
        self.mock_producer.flush.assert_called()

    @patch("lsst.rucioevents.kafka_producer.logger")
    def test_delivery_report_success(self, mock_logger):
        """Trying to test the callback."""
        events = [{"key": "test_key", "value": "test_value"}]
        topic = "test_topic"

        def mock_produce(*args, **kwargs):
            callback = kwargs.get("callback")
            if callback:
                mock_msg = MagicMock()
                mock_msg.topic.return_value = topic
                mock_msg.key.return_value = "test_key"
                mock_msg.partition.return_value = 0
                mock_msg.offset.return_value = 123
                callback(None, mock_msg)

        self.mock_producer.produce.side_effect = mock_produce
        self.kafka_producer.send_event(topic, events)
        mock_logger.info.assert_called_with(
            "Message: test_key successfully produced to Topic: test_topic Partition: [0] at offset 123"
        )


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
