import unittest
import lsst.utils.tests
from unittest.mock import MagicMock, patch
from rucio.common.exception import DataIdentifierNotFound
from lsst.rucioevents.rucio_processor import RucioProcessor


class TestRucioProcessor(unittest.TestCase):
    def setUp(self):
        """Config"""
        self.scope = "test_scope"
        self.name = "test_name"
        self.rse = "test_rse"
        self.mock_client = MagicMock()
        self.processor = RucioProcessor(
            self.scope, self.name, self.rse, self.mock_client
        )

    @patch("lsst.rucioevents.rucio_processor.Client")
    def test_init(self, MockClient):
        """Testing initialization."""
        processor = RucioProcessor(self.scope, self.name, self.rse)
        MockClient.assert_called_once()
        self.assertEqual(processor.scope, self.scope)
        self.assertEqual(processor.name, self.name)
        self.assertEqual(processor.rse, self.rse)

    def test_get_did_info_success(self):
        """Succesfull _get_did_info."""
        dummy_did_info = {
            "scope": "raw",
            "name": "LSSTCam/20240820/MC_P_20240820_000011/MC_P_20240820_000011_R22_S22.fits",
            "type": "FILE",
            "account": "register_service",
            "bytes": 22164480,
            "length": 1,
            "md5": "37c84e19f0b55aae361403f72997328d",
            "adler32": "8cdcf74e",
        }
        self.mock_client.get_did.return_value = dummy_did_info
        result = self.processor._get_did_info()
        self.assertEqual(result, dummy_did_info)
        self.mock_client.get_did.assert_called_once_with(self.scope, self.name)

    def test_get_did_info_not_found(self):
        """Missing DID."""
        self.mock_client.get_did.side_effect = DataIdentifierNotFound
        result = self.processor._get_did_info()
        self.assertIsNone(result)
        self.mock_client.get_did.assert_called_once_with(self.scope, self.name)

    def test_get_files_info_success(self):
        """Succesfull _get_files_info."""
        dummy_files_info = [{"scope": "scope1", "name": "name1"}]
        self.mock_client.list_files.return_value = dummy_files_info
        self.mock_client.get_did.return_value = {"key": "value"}
        result = self.processor._get_files_info()
        self.assertEqual(len(result), 1)
        self.mock_client.list_files.assert_called_once_with(self.scope, self.name)
        self.mock_client.get_did.assert_called_once_with("scope1", "name1")

    def test_get_files_info_not_found(self):
        """Test no DID found"""
        self.mock_client.list_files.side_effect = DataIdentifierNotFound
        result = self.processor._get_files_info()
        self.assertEqual(result, [])
        self.mock_client.list_files.assert_called_once_with(self.scope, self.name)

    def test_get_rubin_payload_success(self):
        """Test succesfully _get_rubin_payload."""
        dummy_metadata = {
            "rubin_butler": 1,
            "rubin_sidecar": "sidecar_data",
            "name": "name",
            "scope": self.scope,
            "dataset": self.name,
            "datasetScope": self.scope,
        }
        self.mock_client.get_metadata.return_value = dummy_metadata
        result = self.processor._get_rubin_payload("test_file")
        self.assertEqual(result, dummy_metadata)
        self.mock_client.get_metadata.assert_called_once_with(
            self.scope, name="test_file", plugin="ALL"
        )

    def test_get_rubin_payload_not_found(self):
        """Test missing Rubin metadata"""
        self.mock_client.get_metadata.side_effect = DataIdentifierNotFound
        result = self.processor._get_rubin_payload("test_file")
        self.assertIsNone(result)

    def test_get_all_metadata(self):
        """Succesfull _get_all_metadata."""
        dummy_rubin_payload = {
            "rubin_butler": 1,
            "rubin_sidecar": "sidecar_data",
            "name": "name",
            "scope": self.scope,
            "dataset": self.name,
            "datasetScope": self.scope,
        }
        self.mock_client.get_metadata.return_value = dummy_rubin_payload
        result = self.processor._get_all_metadata(["file1", "file2"])
        self.assertEqual(len(result), 2)
        self.assertEqual(result["file1"], dummy_rubin_payload)
        self.assertEqual(result["file2"], dummy_rubin_payload)

    def test_get_rse_info_success(self):
        """Succesfull _get_rse_info."""
        dummy_replicas = [
            {
                "name": "file1",
                "rses": {self.rse: ["rse_url1"]},
            },
            {
                "name": "file2",
                "rses": {self.rse: ["rse_url2"]},
            },
        ]
        self.mock_client.list_replicas.return_value = dummy_replicas
        result = self.processor._get_rse_info()
        self.assertEqual(result, {"file1": "rse_url1", "file2": "rse_url2"})

    def test_get_rse_info_rse_not_found(self):
        """Testing missing RSE."""
        dummy_replicas = [
            {
                "name": "file1",
                "rses": {"other_rse": ["other_rse_url"]},
            }
        ]
        self.mock_client.list_replicas.return_value = dummy_replicas
        result = self.processor._get_rse_info()
        self.assertEqual(result, {})

        def test_merge_metadata(self):
            """Succesfull _merge_metadata."""
            self.mock_client.list_files.return_value = [
                {"scope": self.scope, "name": "file1"},
                {"scope": self.scope, "name": "file2"},
            ]
            self.mock_client.get_did.return_value = {"key": "value"}
            self.mock_client.get_metadata.return_value = {"rubin_key": "rubin_value"}
            self.mock_client.list_replicas.return_value = [
                {
                    "name": "file1",
                    "rses": {self.rse: ["rse_url1"]},
                }
            ]
            result = self.processor._merge_metadata()
            self.assertEqual(len(result), 1)
            self.assertEqual(
                result["file1"],
                {
                    "rubin_key": "rubin_value",
                    "dst-url": "rse_url1",
                    "dst-rse": self.rse,
                },
            )

    def test_get_payload(self):
        """Testing get _merge_metadata."""
        dummy_merged_data = {"test": "data"}
        self.processor._merge_metadata = MagicMock(return_value=dummy_merged_data)
        result = self.processor.get_payload()
        self.assertEqual(result, dummy_merged_data)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
