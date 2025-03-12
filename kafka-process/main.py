import logging
from requests_sse import EventSource
from quixstreams import Application
import json
from pprint import pformat


def handle_stats(stats_msg):
    stats = json.loads(stats_msg)
    logging.info("STATS: %s", pformat(stats))


def main():
    logging.info("START")

    app = Application(
        broker_address="localhost:19092",
        loglevel="DEBUG",
        producer_extra_config={
            # measure 1st:
            "statistics.interval.ms": 3 * 1000,
            "stats_cb": handle_stats,
            # low-level debugging messages:
            "debug": "msg",
            # then look at making more efficient
            # determine how long to wait before sending batch:
            "linger.ms": 200,  # useful for improving efficiency. performance vs. latency tradeoff
            "batch.size": 1024 * 1024,  # maximum batch size 1MB
            # readable strings compress very well
            "compression.type": "gzip",  # tradeoff is cpu time to make network more efficient
        },
    )

    with (
        app.get_producer() as producer,
        EventSource(
            "http://github-firehose.libraries.io/events", timeout=30
        ) as event_source,
    ):
        for event in event_source:
            value = json.loads(event.data)  ## turns into python dictionary
            key = value["id"]  ## note the dictionary notation1
            logging.debug(
                "Got: %s", pformat(value)
            )  ## turns back into a string with neat indentation

            ## send to kafka
            producer.produce(
                topic="github_events",
                key=key,
                value=json.dumps(value),
            )


if __name__ == "__main__":
    logging.basicConfig(level="INFO")
    main()
