"""Defines trends calculations for stations"""
import logging
from dataclasses import dataclass

import faust


logger = logging.getLogger(__name__)

TOPIC_PARTITIONS = 2
TOPIC_REPLICAS = 2


@dataclass
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


@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

    @staticmethod
    def from_station(station: Station):
        if station.red:
            line = 'red'
        elif station.blue:
            line = 'blue'
        elif station.green:
            line = 'green'
        else:
            logger.error(f'Invalid line for event: {station}')
            return

        return TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line
        )


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic(
    'com.chicago.cta.stations',
    value_type=Station,
    partitions=TOPIC_PARTITIONS,
    replicas=TOPIC_REPLICAS
)

out_topic = app.topic(
    'com.chicago.cta.stations.table.v1',
    partitions=TOPIC_PARTITIONS,
    replicas=TOPIC_REPLICAS
)

table = app.Table(
    name='com.chicago.cta.stations.table.v1',
    default=TransformedStation,
    partitions=TOPIC_PARTITIONS,
    changelog_topic=out_topic,
)


@app.agent(topic)
async def process_events(events):
    async for event in events:
        transformed_station = TransformedStation.from_station(event)
        if transformed_station:
            table[transformed_station.station_id] = transformed_station


if __name__ == "__main__":
    app.main()
