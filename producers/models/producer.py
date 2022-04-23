"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


class Producer:
    """Defines and provides common functionality amongst Producers"""

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

        self.broker_properties = {
            'bootstrap.servers': BROKER_URL,
            'schema.registry.url': SCHEMA_REGISTRY_URL,
        }

        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            config=self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        admin_client = AdminClient({
            'bootstrap.servers': self.broker_properties['bootstrap.servers']
        })

        if admin_client.list_topics(topic=self.topic_name).topics.get(self.topic_name):
            logger.info("Topic already exists - skipping")
        else:
            admin_client.create_topics(new_topics=[
                NewTopic(self.topic_name, self.num_partitions, self.num_replicas)
            ])

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush(timeout=15)

    @staticmethod
    def time_millis():
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
