import argparse
import logging
from typing import List
from rucio_processor import RucioProcessor
from event_creator import KafkaEvent
from kafka_producer import RucioKafkaProducer
from rucio.client import Client

logging.basicConfig(level=logging.INFO)
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
        help="Specify Kafka topic. Defaults to RSE name if not provided.",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Increase the verbosity level of the output.",
    )
    return parser.parse_args()


def read_dids_from_file(file_path: str) -> List:
    with open(file_path, "r") as file:
        dids = [line.strip() for line in file if line.strip()]
    return dids


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


def main():
    args = parse_arguments()
    topic = args.topic or args.rse
    dids = args.dids or read_dids_from_file(args.file)
    if args.verbose:
        logger.info(f"The following list of DIDs will be processed:  {dids}")
        logger.info(f"The events will be applied to the following RSE: {args.rse}")
        logger.info(f"The events will be sent to the following topic: {topic}")

    process_dids(dids, args.rse, topic)


if __name__ == "__main__":
    main()
