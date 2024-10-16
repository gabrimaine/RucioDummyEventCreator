from typing import Dict, List, Optional
from rucio.client import Client
from rucio.common.exception import DataIdentifierNotFound
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("RucioProcessor")


class RucioProcessor:
    """
    Provides an interface to retrieve DID information from Rucio.

    Args:
        scope (str): The Rucio scope.
        name (str): The Rucio DID name.
        rse (str): The Rucio Storage Element.
        client (Client, optional): A pre-configured Rucio Client object.
            If not provided, a new Client will be created.
    """

    def __init__(
        self, scope: str, name: str, rse: str, client: Optional[Client] = None
    ):
        self.client = client or Client()
        self.scope = scope
        self.name = name
        self.rse = rse

    def get_payload(self) -> Dict:
        """Merge metadata and RSE information into a single payload."""
        return self._merge_metadata()

    def _get_did_info(self) -> Optional[Dict]:
        """Retrieve DID info from Rucio."""
        try:
            return self.client.get_did(self.scope, self.name)
        except DataIdentifierNotFound:
            logger.error(f"DID {self.name} not found in scope {self.scope}.")
        except Exception as e:
            logger.error(f"Error retrieving DID {self.name}: {e}")
        return None

    def _get_files_info(self) -> List[Dict]:
        """Retrieve information about all the files within a DID."""
        try:
            return [
                self.client.get_did(file["scope"], file["name"])
                for file in self.client.list_files(self.scope, self.name)
            ]
        except DataIdentifierNotFound:
            logger.error(f"DID {self.name} not found in scope {self.scope}.")
        except Exception as e:
            logger.error(f"Error retrieving files info for DID {self.name}: {e}")
        return []

    def _get_file_names(self) -> List[str]:
        """Retrieve the names of all files within a DID."""
        logger.info("Getting filenames")
        return [file_info["name"] for file_info in self._get_files_info()]

    def _get_rubin_payload(self, name: str) -> Optional[Dict]:
        """Retrieve the Rubin metadata of a specific file or container."""
        RUBIN_BUTLER = "rubin_butler"
        RUBIN_SIDECAR = "rubin_sidecar"
        logger.info(f"Getting metadata for {name}")
        try:
            metas = self.client.get_metadata(self.scope, name=name, plugin="ALL")
            payload = {
                RUBIN_BUTLER: metas.get(RUBIN_BUTLER),
                RUBIN_SIDECAR: metas.get(RUBIN_SIDECAR),
            }
            if not any(payload.values()):
                logger.warning(
                    f"No relevant metadata found for {name} in scope {self.scope}."
                )
                return None
            return payload
        except DataIdentifierNotFound:
            logger.error(f"Metadata for {name} not found in scope {self.scope}.")
        except Exception as e:
            logger.error(f"Error retrieving metadata for {name}: {e}")
        return None

    def _get_all_metadata(self, names: List[str]) -> Dict[str, Optional[Dict]]:
        """Retrieve metadata for all specified names."""
        return {name: self._get_rubin_payload(name) for name in names}

    def _get_rse_info(self) -> Dict[str, str]:
        """Retrieve RSE information for the specified DID."""
        logger.info("Getting RSEs")
        try:
            replicas = self.client.list_replicas(
                [{"scope": self.scope, "name": self.name}], rse_expression=self.rse
            )
            return {
                replica["name"]: replica["rses"][self.rse][0]
                for replica in replicas
                if self.rse in replica["rses"]
            }
        except Exception as e:
            logger.error(f"Error retrieving RSE info for {self.name}: {e}")
            return {}

    def _merge_metadata(self) -> Dict:
        """Merge Rubin meta and RSE information into a single dictionary."""
        rubin_payload = self._get_all_metadata(self._get_file_names())
        rse_payload = self._get_rse_info()
        merged_dict = {}
        for name, items in rubin_payload.items():
            if items and name in rse_payload:
                merged_dict[name] = {
                    **items,
                    "dst-url": rse_payload[name],  # Add the URL from rse_payload
                    "dst-rse": self.rse,  # Add the RSE
                }
        return merged_dict
