"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse

import requests

from models.producer import Producer


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://localhost:8082"

    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))


    def __init__(self, month):
       
        super().__init__(
            "gov.cta.weather", 
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            # similar to station.py
            num_partitions=4,
            num_replicas=1
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        # Defined schema in weather_value.json
        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        # Complete the function by posting a weather event to REST Proxy. Make sure to
        # specify the Avro schemas and verify that you are using the correct Content-Type header.
        #
        #logger.info("weather kafka proxy integration incomplete - skipping")
        resp = requests.post(
            
            f"{Weather.rest_proxy_url}/topics/gov.cta.weather",
            # content header - application.vnd.kafka.<FORMAT>.v2+json
            # specified to use avro schema so content type should be related to avro.
            # https://docs.confluent.io/platform/current/kafka-rest/api.html#errors
            
            headers={"Content-Type": "application/vnd.kafka.avro.v2+json"},
            
            # specifying the avro schema below
            # https://docs.confluent.io/platform/current/kafka-rest/api.html#post--topics-(string-topic_name)
            data=json.dumps(
                {
                    "key_schema": json.dumps(Weather.key_schema),
                    "value_schema": json.dumps(Weather.value_schema),
                    "records": 
                    [
                        {
                            "key": {"timestamp": self.time_millis()},
                            "value": 
                            {
                                "temperature": self.temp, 
                                "status": self.status.name
                            }
                        }
                    ]
                   
                }
            )
        )
        try:
            resp.raise_for_status()
        except: 
            logger.error(f"Post to REST Proxy failed. {json.dumps(resp.json())}")

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
        print(f"weather.py, topic_name-gov.cta.weather, temperature= {self.temp}, status={self.status}")
