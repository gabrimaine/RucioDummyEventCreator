import logging
from typing import Dict
from confluent_kafka import Producer
from config import KafkaConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RucioKafkaProducer:
    def __init__(self, topic: str):
        """
        Initializes a Kafka producer to send fakes Rucio events.

        :param topic: The name of the Kafka topic.
        """
        config = KafkaConfig()
        self.producer = Producer(config.complete_config())
        self.topic = topic

    def send_event(self, event: Dict) -> None:
        """
        Sends an event to Kafka.

        :param event: Dictionary containing the event data.
        """

        def delivery_report(errmsg, msg):
            """
            Reports the Failure or Success of a message delivery.
            Args:
                errmsg  (KafkaError): The Error that occurred while message producing.
                msg    (Actual message): The message that was produced.
            Note:
                In the delivery report callback the Message.key() and Message.value()
                will be the binary format as encoded by any configured Serializers and
                not the same object that was passed to produce().
                If you wish to pass the original object(s) for key and value to delivery
                report callback we recommend a bound callback or lambda where you pass
                the objects along.
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

        self.producer.produce(
            topic=self.topic,
            key=str(
                event.get("key", "default_key")
            ),  # Use a key if available, otherwise use a default
            value=str(
                event
            ),  # Convert the event to a string or serialize it appropriately
            callback=delivery_report,
        )
        self.producer.flush()
