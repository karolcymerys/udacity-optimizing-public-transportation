"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"


KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id      BIGINT,
    station_name    STRING,
    line            STRING
) WITH (
    kafka_topic = 'com.chicago.cta.turnstiles.v1',
    value_format = 'avro',
    key = 'station_id'
);

CREATE TABLE turnstile_summary
    WITH (value_format = 'json') AS
        SELECT station_id AS station_id,
               COUNT(*)   AS count
            FROM turnstile
            GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("turnstile_summary") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
