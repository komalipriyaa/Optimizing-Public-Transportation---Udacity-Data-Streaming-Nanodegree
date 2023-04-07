"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# Input Kafka Topic
topic = app.topic("station.stations", value_type=Station)
# Output Kafka Topic
out_topic = app.topic("station.stations.faust", partitions=1)
# https://faust.readthedocs.io/en/latest/userguide/tables.html#basics
fausttable = app.Table(
    "station.stations.faust.table",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


# Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
# https://faust.readthedocs.io/en/latest/userguide/tables.html#id2
@app.agent(topic)
async def faust_station(stations):
    async for st in stations:
        newline = ''
        if st.red:
            newline='red'
        elif st.blue:
            newline='blue'
        elif st.green:
            newline='green'
        else:
            newline='None'
        fausttable[st.station_id]=TransformedStation(station_id=st.station_id, station_name=st.station_name, order=st.order, line=newline)


if __name__ == "__main__":
    app.main()
