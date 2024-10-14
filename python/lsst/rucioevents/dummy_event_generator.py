import argparse
import logging
from typing import List
from rucio_processor import RucioProcessor
from event_creator import KafkaEvent
from kafka_producer import RucioKafkaProducer
from rucio.client import Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Process a list of DIDs and send events to Kafka."
    )
    parser.add_argument(
        "-d",
        "--dids",
        metavar="DID",
        type=str,
        nargs="+",
        help='List of DIDs in the format "scope:name".',
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
        help="Specify the Kafka topic for event ingestion. Defaults to the same name as the RSE if not provided.",
    )
    return parser.parse_args()


def process_dids(dids: List, rse: str, topic: str):
    client = Client()
    kafka_sender = RucioKafkaProducer(topic)

    for did in dids:
        scope, name = did.split(":")
        try:
            rucio_client = RucioProcessor(scope, name, rse, client)
            # Estrai i metadati
            payload = rucio_client.get_payload()
            event_gen = KafkaEvent(payload)
            kafka_sender.send_event(event_gen.process_metadata())

            logger.info(f"DID {did} dummy event correctly created and sent")

        except Exception as e:
            logger.error(f"Error processing DID {did}: {e}")


if __name__ == "__main__":
    args = parse_arguments()
    topic = args.topic or args.rse
    process_dids(args.dids, args.rse, topic)
