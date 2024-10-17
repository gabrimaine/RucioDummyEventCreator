import unittest
import json
import uuid
import lsst.utils.tests
from unittest.mock import MagicMock, patch
from lsst.rucioevents.kafka_producer import RucioKafkaProducer


class TestRucioKafkaProducer(unittest.TestCase):
    def setUp(self):
        """Configurazione comune per i test."""
        self.topic = "test_topic"
        self.mock_producer = MagicMock()
        self.kafka_producer = RucioKafkaProducer(self.topic)
        # Sostituisci il producer reale con il mock
        self.kafka_producer.producer = self.mock_producer

    def test_init(self):
        """Verifica che l'inizializzazione funzioni correttamente."""
        self.assertEqual(self.kafka_producer.topic, self.topic)

    def test_send_event_success(self):
        """Verifica l'invio di un evento con successo."""
        event = {"key": "test_key", "data": "test_data"}
        self.kafka_producer.send_event([event])
        self.mock_producer.produce.assert_called_once_with(
            topic=self.topic,
            key=event["key"],
            value=json.dumps(event),
            callback=unittest.mock.ANY,  # I don't know how to test directly the callback
        )
        self.mock_producer.flush.assert_called_once()

    def test_send_event_no_key(self):
        """Verifica l'invio di un evento senza chiave)."""
        event = {"data": "test_data"}
        self.kafka_producer.send_event([event])
        self.mock_producer.produce.assert_called_once()
        args, kwargs = self.mock_producer.produce.call_args
        self.assertEqual(kwargs["topic"], self.topic)
        key = kwargs["key"]
        if isinstance(key, bytes):
            key = key.decode("utf-8")
        if key.startswith("b'") and key.endswith("'"):
            key = key[2:-1]
        self.assertIsInstance(uuid.UUID(key), uuid.UUID)
        self.assertEqual(kwargs["value"], json.dumps(event))
        self.mock_producer.flush.assert_called_once()

    def test_send_event_multiple(self):
        """Verifica l'invio di più eventi."""
        events = [
            {"key": "key1", "data": "data1"},
            {"key": "key2", "data": "data2"},
        ]
        self.kafka_producer.send_event(events)
        self.assertEqual(self.mock_producer.produce.call_count, 2)
        self.mock_producer.flush.assert_called()

    @patch("lsst.rucioevents.kafka_producer.logger")
    def test_delivery_report_success(self, mock_logger):
        """Verifica il caso di successo del delivery report."""
        events = [{"key": "test_key", "value": "test_value"}]

        def mock_produce(*args, **kwargs):
            callback = kwargs.get("callback")
            if callback:
                mock_msg = MagicMock()
                mock_msg.key.return_value = "test_key"
                mock_msg.topic.return_value = self.topic
                mock_msg.partition.return_value = 0
                mock_msg.offset.return_value = 123
                callback(None, mock_msg)

        self.mock_producer.produce.side_effect = mock_produce
        self.kafka_producer.send_event(events)
        mock_logger.info.assert_called_with(
            "Message: test_key successfully produced to Topic: test_topic Partition: [0] at offset 123"
        )

        # def mock_produce_error(*args, **kwargs):
        #    callback = kwargs.get("callback")
        #    if callback:
        #        mock_msg = MagicMock()
        #        mock_msg.key.return_value = "test_key"
        #        callback("Error", mock_msg)

        # self.mock_producer.producer.produce.side_effect = mock_produce_error

        # self.mock_producer.send_event(events)

        # mock_logger.error.assert_called_with(
        #    "Delivery failed for Message: test_key : Error"
        # )


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
