import unittest
import argparse
import lsst.utils.tests
from unittest.mock import patch, mock_open
from lsst.rucioevents.dummy_event_generator import (
    parse_arguments,
    read_dids_from_file,
    process_dids,
    main,
)


class TestDummyEventGenerator(unittest.TestCase):
    @patch("argparse.ArgumentParser.parse_args")
    def test_parse_arguments_dids(self, mock_parse_args):
        """Check parsing with --dids."""
        mock_parse_args.return_value = argparse.Namespace(
            dids=["scope1:name1", "scope2:name2"],
            file=None,
            rse="test_rse",
            topic=None,
            verbose=False,
        )
        args = parse_arguments()
        self.assertEqual(args.dids, ["scope1:name1", "scope2:name2"])
        self.assertIsNone(args.file)
        self.assertEqual(args.rse, "test_rse")
        self.assertIsNone(args.topic)
        self.assertFalse(args.verbose)

    @patch("argparse.ArgumentParser.parse_args")
    def test_parse_arguments_file(self, mock_parse_args):
        """Check parsing with --file."""
        mock_parse_args.return_value = argparse.Namespace(
            dids=None,
            file="test_file.txt",
            rse="test_rse",
            topic="test_topic",
            verbose=True,
        )
        args = parse_arguments()
        self.assertIsNone(args.dids)
        self.assertEqual(args.file, "test_file.txt")
        self.assertEqual(args.rse, "test_rse")
        self.assertEqual(args.topic, "test_topic")
        self.assertTrue(args.verbose)

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data="scope1:name1\nscope2:name2\n",
    )
    def test_read_dids_from_file(self, mock_file):
        """Check DID from a file."""
        dids = read_dids_from_file("test_file.txt")
        self.assertEqual(dids, ["scope1:name1", "scope2:name2"])
        mock_file.assert_called_once_with("test_file.txt", "r")

    @patch("lsst.rucioevents.dummy_event_generator.RucioKafkaProducer")
    @patch("lsst.rucioevents.dummy_event_generator.KafkaEvent")
    @patch("lsst.rucioevents.dummy_event_generator.RucioProcessor")
    @patch("lsst.rucioevents.dummy_event_generator.Client")
    def test_process_dids(
        self, mock_client, mock_processor, mock_event, mock_kafka_producer
    ):
        """Check DID analysis."""
        dids = ["scope1:name1", "scope2:name2"]
        rse = "test_rse"
        topic = "test_topic"

        mock_processor_instance = mock_processor.return_value
        mock_processor_instance.get_payload.return_value = {"test": "payload"}

        mock_event_instance = mock_event.return_value
        mock_event_instance.process_metadata.return_value = [{"event": "data"}]

        process_dids(dids, rse, topic)

        self.assertEqual(mock_processor.call_count, 2)
        mock_processor.assert_any_call("scope1", "name1", rse, mock_client.return_value)
        mock_processor.assert_any_call("scope2", "name2", rse, mock_client.return_value)

        self.assertEqual(mock_event.call_count, 2)
        mock_event.assert_called_with({"test": "payload"})

        mock_kafka_producer.return_value.send_event.assert_called_with(
            [{"event": "data"}]
        )

    @patch("lsst.rucioevents.dummy_event_generator.process_dids")
    @patch("lsst.rucioevents.dummy_event_generator.read_dids_from_file")
    @patch("lsst.rucioevents.dummy_event_generator.parse_arguments")
    def test_main_dids(self, mock_parse_args, mock_read_dids, mock_process_dids):
        """Check Process passing --dids."""
        mock_parse_args.return_value = argparse.Namespace(
            dids=["scope1:name1"], file=None, rse="test_rse", topic=None, verbose=False
        )
        main()
        mock_process_dids.assert_called_once_with(
            ["scope1:name1"], "test_rse", "test_rse"
        )

    @patch("lsst.rucioevents.dummy_event_generator.process_dids")
    @patch("lsst.rucioevents.dummy_event_generator.read_dids_from_file")
    @patch("lsst.rucioevents.dummy_event_generator.parse_arguments")
    def test_main_file(self, mock_parse_args, mock_read_dids, mock_process_dids):
        """Check Process passing --file."""
        mock_parse_args.return_value = argparse.Namespace(
            dids=None,
            file="test_file.txt",
            rse="test_rse",
            topic="test_topic",
            verbose=False,
        )
        mock_read_dids.return_value = ["scope1:name1"]
        main()
        mock_process_dids.assert_called_once_with(
            ["scope1:name1"], "test_rse", "test_topic"
        )


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
