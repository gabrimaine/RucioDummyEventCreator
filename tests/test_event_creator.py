import unittest
import lsst.utils.tests
from datetime import datetime
from unittest.mock import patch
from lsst.rucioevents.event_creator import KafkaEvent


class TestKafkaEvent(unittest.TestCase):
    def setUp(self):
        """Set-up the config."""
        self.dummy_metadata = {
            "file1.txt": {
                "name": "file1.txt",
                "scope": "test_scope",
                "dataset": "test_dataset",
                "datasetScope": "test_dataset_scope",
                "dst-rse": "test_rse",
                "dst-url": "test_url",
                "rubin_butler": "test_butler",
                "rubin_sidecar": "test_sidecar",
            },
            "file2.txt": {
                "name": "file2.txt",
                "scope": "test_scope",
                "dataset": "test_dataset",
                "datasetScope": "test_dataset_scope",
                "dst-rse": "test_rse",
                "dst-url": "test_url",
                "rubin_butler": "test_butler",
                "rubin_sidecar": "test_sidecar",
            },
        }
        self.kafka_event = KafkaEvent(self.dummy_metadata)

    def test_get_metadata(self):
        """Check metadata."""
        self.assertEqual(self.kafka_event.get_metadata(), self.dummy_metadata)

    @patch("lsst.rucioevents.event_creator.logger.info")
    def test_process_metadata(self, mock_logger):
        """Chech events creation from dummy metadata."""
        events = self.kafka_event.process_metadata()
        self.assertEqual(len(events), 2)  # Due file nei metadati fittizi

        for event in events:
            self.assertEqual(event["event_type"], KafkaEvent.EVENT_TYPE)
            self.assertIn(
                event["payload"]["name"],
                self.dummy_metadata.get(event["payload"]["name"]).get("name"),
            )
            self.assertIsInstance(
                datetime.strptime(event["created_at"], KafkaEvent.DATE_FORMAT),
                datetime,
            )

        mock_logger.assert_called_once_with("Generating dummy events")

    def test_create_file_event(self):
        """Check single file event."""
        file_meta = {
            "name": "file1.txt",
            "scope": "test_scope",
            "dataset": "test_dataset",
            "datasetScope": "test_dataset_scope",
            "dst-rse": "test_rse",
            "dst-url": "test_url",
            "rubin_butler": "test_butler",
            "rubin_sidecar": "test_sidecar",
        }
        event = self.kafka_event._create_file_event(file_meta)
        self.assertEqual(event["event_type"], KafkaEvent.EVENT_TYPE)
        self.assertEqual(event["payload"], file_meta)
        self.assertIsInstance(
            datetime.strptime(event["created_at"], KafkaEvent.DATE_FORMAT), datetime
        )

    def test_get_template(self):
        """Check get_template method."""
        template = KafkaEvent._get_template()
        self.assertEqual(
            list(template.keys()),
            [
                "name",
                "scope",
                "dataset",
                "datasetScope",
                "dst-rse",
                "dst-url",
                "rubin_butler",
                "rubin_sidecar",
            ],
        )


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
