"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    # TODO: Complete the Kafka Connect Config below.
    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    # Make sure to think about what an appropriate topic prefix would be, and how frequently Kafka
    # Connect should run this connector (hint: not very often!)
    #logger.info("connector code not completed skipping connector creation")
    
    # https://docs.confluent.io/kafka-connectors/jdbc/current/source-connector/source_config_options.html#jdbc-source-connector-configuration-properties
    
    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "batch.max.rows": "500",
                # from the project description
                "connection.url": "jdbc:postgresql://localhost:5432/cta",
                "connection.user": "cta_admin",
                "connection.password": "chicago",
                # whitelisting stations table as specified
                "table.whitelist": "stations",
                # https://docs.confluent.io/kafka-connectors/jdbc/current/source-connector/source_config_options.html#mode
                "mode": "incrementing",
                "incrementing.column.name": "stop_id",
                "topic.prefix": "station.",
                # provided : not very often - so considering one hour = 3600 seconds
                "poll.interval.ms": "3600000",
            }
        }),
    )

    ## Ensure a healthy response was given
    try:
        resp.raise_for_status()
    except:
            print(f"Failed to fetch records: {json.dumps(resp.json(), indent=2)}")
            logging.error("Failed to fetch stations data through JDBC.")
            #return
    #logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
