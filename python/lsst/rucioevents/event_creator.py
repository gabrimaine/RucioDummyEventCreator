from typing import Dict, List
from datetime import datetime


class KafkaEvent:
    """Class to handle Kafka events for file transfers."""

    EVENT_TYPE = "transfer-done"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

    def __init__(self, metadata: Dict[str, Dict]):
        self.metadata = metadata

    def get_metadata(self) -> Dict[str, Dict]:
        """Return the metadata associated with the event."""
        return self.metadata

    def process_metadata(self) -> List[Dict]:
        """Process metadata to create a list of events."""
        all_events = []
        for file, payload in self.metadata.items():
            all_events.append(self._create_file_event(payload))
        return all_events

    def _create_file_event(self, file_meta: Dict) -> Dict:
        """Create a single file event with the given metadata."""
        # payload = self._get_template()
        # payload.update(file_meta)
        created_at = datetime.now().strftime(self.DATE_FORMAT)
        event_dict = {
            "event_type": self.EVENT_TYPE,
            "payload": file_meta,
            "created_at": created_at,
        }
        return event_dict

    @staticmethod
    def _get_template() -> Dict:
        """Return a simplified payload template."""
        payload_simplified = {
            "dst-rse": "",
            "dst-url": "",
            "rubin_butler": "",
            "rubin_sidecar": "",
        }
        return payload_simplified
