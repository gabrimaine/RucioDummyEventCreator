import argparse
import logging
from typing import List
from rucio.client import Client
from lsst.rucioevents.rucio_processor import RucioProcessor
from lsst.rucioevents.event_creator import KafkaEvent
from lsst.rucioevents.kafka_producer import RucioKafkaProducer

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("RucioDummyEventGenerator")


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Process a list of DIDs and send events to Kafka."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-d",
        "--dids",
        metavar="DID",
        type=str,
        nargs="+",
        help='List of DIDs in the format "scope:name".',
    )
    group.add_argument(
        "-f",
        "--file",
        metavar="FILE",
        type=str,
        help='Path to a file containing a list of DIDs in the format "scope:name", one per line.',
    )
    parser.add_argument(
        "-r",
        "--rse",
        metavar="RSE",
        type=str,
        required=True,
        help="Specify the RSE to be used for processing.",
    )

    parser.add_argument(
        "-t",
        "--topic",
        metavar="TOPIC",
        type=str,
        required=False,
        help="Specify Kafka topic. Defaults to RSE+scope as RSE-scope if not provided.",
    )

    parser.add_argument(
        "--dry",
        action="store_true",
        required=False,
        help="Print the generated event without send it.",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Increase the verbosity level of the output.",
    )
    return parser.parse_args()


def read_dids_from_file(file_path: str) -> List:
    """
    Read Data Identifiers (DIDs) from a specified file and return
    them as a list of strings.

    Args:
        file_path (str): The path to the file containing the DIDs.
        Each DID should be on a separate line.

    Returns:
        List[str]: A list of DIDs, where each DID
        is a non-empty string read from the file.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        IOError: If there is an issue opening or reading from the file.
    """
    with open(file_path, "r") as file:
        dids = [line.strip() for line in file if line.strip()]
    return dids


def process_dids(dids: List, rse: str, topic: str, dry=False):
    """
    Process a list of DIDs (Data Identifiers) to generate
    and optionally send events to a Kafka topic.

    Args:
        dids (List): A list of data identifier strings
            each formatted as "scope:name".
        rse (str): The Rucio Storage Element (RSE)
            to be associated with the DIDs.
        topic (str): The Kafka topic to which the events will be sent.
            If None, it defaults to "{rse}-{scope}".
        dry (bool): A flag indicating whether to perform a dry run (True)
            where events are created but not sent, or a real run (False).

    Raises:
        Exception: If any error occurs during the processing of a DID.
    """
    client = Client()
    kafka_sender = RucioKafkaProducer()

    for did in dids:
        scope, name = did.split(":")
        if topic is None:
            topic = f"{rse}-{scope}"
        logger.info(f"The events will be sent to the following topic: {topic}")
        try:
            rucio_client = RucioProcessor(scope, name, rse, client)
            payload = rucio_client.get_payload()
            if len(payload) == 0:
                logger.warning(
                    f"No RSE found associated with files in DID {scope}:{name}"
                )
                logger.warning(f"Event creation for DID {scope}:{name} stopped")
                continue
            event_gen = KafkaEvent(payload)
            if dry:
                logger.info(
                    f"The following event(s) created but not sent  {event_gen.process_metadata()}"
                )
            else:
                kafka_sender.send_event(topic, event_gen.process_metadata())
                logger.info(
                    f"DID {did} dummy event correctly created and sent to the topic {topic}"
                )

        except Exception as e:
            logger.error(f"Error processing DID {did}: {e}")


def main():
    args = parse_arguments()
    topic = args.topic or None
    dids = args.dids or read_dids_from_file(args.file)
    if args.verbose:
        logger.info(f"The following list of DIDs will be processed:  {dids}")
        logger.info(f"The events will be applied to the following RSE: {args.rse}")

    process_dids(dids, args.rse, topic, args.dry)
