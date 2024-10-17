import unittest
import lsst.utils.tests
from lsst.rucioevents.config import KafkaConfig


class TestKafkaConfig(unittest.TestCase):
    def test_init_default_bootstrap_servers(self):
        """Testing KafkaConfig with default servers."""
        config = KafkaConfig()
        self.assertEqual(
            config.bootstrap_servers, KafkaConfig.DEFAULT_BOOTSTRAP_SERVERS
        )
        self.assertEqual(
            config.bootstrap, ",".join(KafkaConfig.DEFAULT_BOOTSTRAP_SERVERS)
        )

    def test_init_custom_bootstrap_servers(self):
        """Testing KafkaConfig with non default servers."""
        bootstrap_servers = ["kafka1:9092", "kafka2:9092"]
        config = KafkaConfig(bootstrap_servers=bootstrap_servers)
        self.assertEqual(config.bootstrap_servers, bootstrap_servers)
        self.assertEqual(config.bootstrap, ",".join(bootstrap_servers))

    def test_get(self):
        """Testing KafkaConfig parameters."""
        config = KafkaConfig(client_id="my-client", acks="all")
        self.assertEqual(config.get("client_id"), "my-client")
        self.assertEqual(config.get("acks"), "all")
        self.assertIsNone(config.get("nonexistent_key"))
        self.assertEqual(
            config.get("nonexistent_key", "default_value"), "default_value"
        )

    def test_complete_config(self):
        """Check config creation."""
        config = KafkaConfig(
            bootstrap_servers=["kafka1:9092", "kafka2:9092"],
            client_id="my-client",
            acks="all",
        )
        expected_config = {
            "bootstrap.servers": "kafka1:9092,kafka2:9092",
            "client_id": "my-client",
            "acks": "all",
        }
        self.assertEqual(config.complete_config(), expected_config)

    def test_str(self):
        """Check config string representation."""
        config = KafkaConfig(bootstrap_servers=["kafka1:9092"], client_id="my-client")
        expected_str = "KafkaConfig(bootstrap_servers=['kafka1:9092'], **{'client_id': 'my-client'})"
        self.assertEqual(str(config), expected_str)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
