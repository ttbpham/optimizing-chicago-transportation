"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # Set broker properties

        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://127.0.0.1:9092",
            "schema.registry.url": "http://0.0.0.0:8081",
        }

        # Create topic if not existing
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=key_schema,
            default_value_schema=value_schema,
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        client = AdminClient({"bootstrap.servers": "PLAINTEXT://127.0.0.1:9092"})

        futures = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=1,
                    replication_factor=1,
                )
            ]
        )

        for topic, future in futures.items():
            try:
                future.result()
                logger.info("topic creation kafka integration complete")
            except Exception as e:
                logger.info(f"failed to create topic {self.topic_name}: {e}")



    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""

        self.close()
        logger.info("producer is closed")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
