"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        logger.info(f"Topic_name in consumer py: {self.topic_name_pattern}")
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.avro.AvroConsumer
        self.broker_properties = {
                "bootstrap.servers": "PLAINTEXT://localhost:9092",
                "group.id": f"{self.topic_name_pattern}",
                # https://medium.com/lydtech-consulting/kafka-consumer-auto-offset-reset-d3962bad2665#:~:text=The%20auto%20offset%20reset%20consumer%20configuration%20defines%20how%20a%20consumer,topic%20for%20the%20first%20time.
                #
                "default.topic.config": {"auto.offset.reset": "earliest" if self.offset_earliest else "latest"}
        }

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)
            

        #
        #
        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        #
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.subscribe
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign )

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        #logger.info("on_assign is incomplete - skipping")
        for partition in partitions:
            if self.offset_earliest is True:
                partition.offset= OFFSET_BEGINNING # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.TopicPartition
            

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        #
        #
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        #
        #
        #logger.info("_consume is incomplete - skipping")
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.poll
        try:
            msg = self.consumer.poll(timeout=self.consume_timeout)
        except e:
            logger.info(f"Consumer poll exception...! {e}")
        if msg is None:
            return 0
        elif msg.error() is not None:
            return 0
        else:
            try:
                self.message_handler(msg)
                #print("message handled in consumer py")
                #print(f"msg value: {msg.value()}")
            except:
                logger.info("Error in message handler!!")
            return 1
        #except:
        #    logger.info("Exception!!!")
        #    return 0


    def close(self):
        """Cleans up any open kafka consumers"""
        #
        #
        # TODO: Cleanup the kafka consumer
        #
        #https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.close
        self.consumer.close()
