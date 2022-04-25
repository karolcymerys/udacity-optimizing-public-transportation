"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)

TOPIC_PARTITIONS = 2
TOPIC_REPLICAS = 2


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        super().__init__(
            topic_name='com.chicago.cta.turnstiles.v1',
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=TOPIC_PARTITIONS,
            num_replicas=TOPIC_REPLICAS,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        self.turnstile_hardware.get_entries(timestamp, time_step)
        self.producer.produce(
            topic=self.topic_name,
            key={"timestamp": self.time_millis()},
            value={
                'station_id': self.station.station_id,
                'station_name': self.station.name,
                'line': self.station.color.name,
            }
        )
