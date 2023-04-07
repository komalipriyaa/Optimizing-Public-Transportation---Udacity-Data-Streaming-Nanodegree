"""Methods pertaining to loading and configuring CTA "L" station data."""
import logging
from pathlib import Path

from confluent_kafka import avro

from models import Turnstile
from models.producer import Producer


logger = logging.getLogger(__name__)


class Station(Producer):
    """Defines a single station"""

    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_key.json")
    # Schema is defined in arrival_value.json
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_value.json")

    def __init__(self, station_id, name, color, direction_a=None, direction_b=None):
        self.name = name
        station_name = (
            self.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        super().__init__(
            topic_name=f"gov.cta.station.{station_name}.arrival",
            key_schema=Station.key_schema,
            value_schema=Station.value_schema, # TODO: Uncomment once schema is defined
            # TODO: num_partitions=???,
            #https://learn.udacity.com/nanodegrees/nd029/parts/cfa24d55-3ab1-4410-8a26-01d9a568564e/lessons/66f0250d-5c3c-4fdf-883e-7ecdf6ea5b98/concepts/b9e7f848-19c5-4cfa-9732-632e4195119b
            # https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster/
            # no. of producers in the architecture= 3 - station, turnstile, weather
            # no. of consumers = 4 - weather, stationtable, atationArrivals, TurnstileSummary
            # overall throughput - Not sure - say 100Mbps
            # producer, consumer throughput - say 10Mbps each
            # no. of partitions=Max(100/3*10, 100/4*10)=MAX(3.33, 2.5)~ 4
            num_partitions=4,
            # https://hevodata.com/learn/kafka-replication-factor-a-comprehensive-guide/#:~:text=To%20ensure%20data%20security%20it,event%20of%20a%20server%20failure.
            # TODO: num_replicas=???,
            num_replicas=1
        )

        self.station_id = int(station_id)
        self.color = color
        self.dir_a = direction_a
        self.dir_b = direction_b
        self.a_train = None
        self.b_train = None
        self.turnstile = Turnstile(self)


    def run(self, train, direction, prev_station_id, prev_direction):
        """Simulates train arrivals at this station"""
        
        # Complete this function by producing an arrival message to Kafka
        
        print("Topic name in station: ", self.topic_name)
        try:
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": self.time_millis()},
                value={
                    "station_id" : self.station_id ,
                    "train_id" : train.train_id, # train_id is from self.tain_id of train.py
                    "direction" : direction,
                    "line" : self.color.name, #.name from line.py
                    "train_status" : train.status.name, #.name from train.py 
                    "prev_station_id" : prev_station_id, 
                    "prev_direction" : prev_direction
                }
            )
        except:
            logger.info("Failed to produce station")

    def __str__(self):
        return "Station | {:^5} | {:<30} | Direction A: | {:^5} | departing to {:<30} | Direction B: | {:^5} | departing to {:<30} | ".format(
            self.station_id,
            self.name,
            self.a_train.train_id if self.a_train is not None else "---",
            self.dir_a.name if self.dir_a is not None else "---",
            self.b_train.train_id if self.b_train is not None else "---",
            self.dir_b.name if self.dir_b is not None else "---",
        )

    def __repr__(self):
        return str(self)

    def arrive_a(self, train, prev_station_id, prev_direction):
        """Denotes a train arrival at this station in the 'a' direction"""
        self.a_train = train
        self.run(train, "a", prev_station_id, prev_direction)

    def arrive_b(self, train, prev_station_id, prev_direction):
        """Denotes a train arrival at this station in the 'b' direction"""
        self.b_train = train
        self.run(train, "b", prev_station_id, prev_direction)

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.turnstile.close()
        super(Station, self).close()
