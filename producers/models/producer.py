"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroConsumer, AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

SCHEMA_REGISTRY_URL = "http://localhost:8081"


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

        # Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        print("Topic: ", self.topic_name)
        self.broker_properties = {
            "bootstrap.servers" : "PLAINTEXT://localhost:9092", 
            "schema.registry.url" : SCHEMA_REGISTRY_URL,
            #"default.topic.config": {"acks": "all"}
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema = self.key_schema,
            default_value_schema = self.value_schema)
    
 

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        
        client = AdminClient({"bootstrap.servers": self.broker_properties["bootstrap.servers"]})
        topic_metadata = client.list_topics(timeout=5)
        if self.topic_name in topic_metadata.topics:
            logger.info(f"topic exits {self.topic_name}")
            futures=None
            
        else:
            
            futures = client.create_topics([
                NewTopic(
                    topic = self.topic_name,
                    num_partitions = self.num_partitions, 
                    replication_factor = self.num_replicas,
                    config ={
                                'cleanup.policy' : 'compact',
                                'compression.type' : 'lz4', 
                            }
                )
            ])
        if futures is not None:    
            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"topic {self.topic_name} created")
                except Exception as e:
                    logger.info(f"failed to create topic {self.topic_name}: {e}")
                    raise
            #logger.info("topic creation kafka integration incomplete - skipping")
            logger.info("topic creation successful")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        #logger.info("producer close incomplete - skipping")
        if self.topic_name is not None:
            self.producer.flush()
            logger.info("Producer is cleaned up")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
