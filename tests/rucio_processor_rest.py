import unittest
import lsst.utils.tests
from unittest.mock import MagicMock, patch
from rucio.common.exception import DataIdentifierNotFound
from rucio_processor import RucioProcessor


class TestRucioProcessor(unittest.TestCase):
    @patch("rucio.client.Client")
    def test_get_payload(self, MockClient):
        # Creazione di un'istanza MagicMock per il client Rucio
        mock_client = MagicMock()

        # Configurazione del mock per restituire valori specifici
        mock_client.get_did.return_value = {"scope": "test_scope", "name": "test_name"}
        mock_client.list_files.return_value = [{"name": "file1"}, {"name": "file2"}]
        mock_client.get_metadata.return_value = {"key": "value"}
        mock_client.list_replicas.return_value = [{"rse": "RSE1"}, {"rse": "RSE2"}]

        # Configurazione del mock per sollevare un'eccezione
        mock_client.get_did.side_effect = DataIdentifierNotFound

        # Sostituzione del client reale con il mock
        MockClient.return_value = mock_client

        # Creazione di un'istanza di RucioProcessor
        processor = RucioProcessor()

        # Esecuzione del metodo da testare
        with self.assertRaises(DataIdentifierNotFound):
            processor.get_payload("test_scope", "test_name")

        # Verifica che i metodi siano stati chiamati correttamente
        mock_client.get_did.assert_called_once_with("test_scope", "test_name")
        mock_client.list_files.assert_not_called()  # Non chiamato a causa dell'eccezione


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
