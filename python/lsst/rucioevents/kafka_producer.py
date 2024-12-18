import logging
import uuid
import json
from typing import Dict
from confluent_kafka import Producer
from lsst.rucioevents.config import KafkaConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RucioKafkaProducer")


class RucioKafkaProducer:
    def __init__(self, topic: str):
        """
        Initializes a Kafka producer to send fakes Rucio events.

        :param topic: The name of the Kafka topic.
        """
        config = KafkaConfig()
        self.producer = Producer(config.complete_config())
        self.topic = topic

    def send_event(self, events: Dict) -> None:
        """
        Sends an event to Kafka.

        :param event: Dictionary containing the event data.
        """

        def delivery_report(errmsg, msg):
            """
            Reports the Failure or Success of a message delivery.
            Args:
                errmsg  (KafkaError): The Error that occurred.
                msg    (Actual message): The message that was produced.
            """

            if errmsg is not None:
                logger.error(
                    "Delivery failed for Message: {} : {}".format(msg.key(), errmsg)
                )
                return
            logger.info(
                "Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}".format(
                    msg.key(), msg.topic(), msg.partition(), msg.offset()
                )
            )

        for event in events:
            default_key = str(uuid.uuid4()).encode("utf-8")
            self.producer.produce(
                topic=self.topic,
                key=event.get("key", default_key),
                value=json.dumps(event),
                callback=delivery_report,
            )
            self.producer.flush()
