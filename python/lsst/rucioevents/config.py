from typing import List, Optional, Any, Dict


class KafkaConfig:
    """Class to manage Kafka configuration."""

    DEFAULT_BOOTSTRAP_SERVERS = ["134.79.23.221:9094"]

    def __init__(self, bootstrap_servers: Optional[List[str]] = None, **kwargs):
        """
        Initializes a new instance of KafkaConfig.

        Args:
            bootstrap_servers: A list of Kafka server addresses in the format "host:port".
                               If None, the default value will be used.
            **kwargs: Additional configuration parameters.
        """
        self.bootstrap_servers = bootstrap_servers or self.DEFAULT_BOOTSTRAP_SERVERS
        self._config = kwargs

    @property
    def bootstrap(self) -> str:
        """Returns a string with the bootstrap server addresses separated by commas."""
        return ",".join(self.bootstrap_servers)

    def get(self, key: str, default=None):
        """Retrieves a configuration parameter."""
        return self._config.get(key, default)

    def complete_config(self) -> Dict[str, Any]:
        """Returns the complete configuration for the Producer."""
        config = {"bootstrap.servers": self.bootstrap}
        config.update(self._config)
        return config

    def __str__(self):
        """String representation of the configuration."""
        return (
            f"KafkaConfig(bootstrap_servers={self.bootstrap_servers}, **{self._config})"
        )
