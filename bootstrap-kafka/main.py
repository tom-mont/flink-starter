from random import choice
from confluent_kafka import Producer
import json
import logging
from pprint import pformat
from requests_sse import EventSource

logger = logging.getLogger(__name__)


def delivery_callback(err, msg):
    if err:
        logging.error("ERROR: Message failed delivery: {}".format(err))
    else:
        logger.info(
            "Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(),
                key=msg.key().decode("utf-8"),
                value=msg.value().decode("utf-8"),
            )
        )


def main():
    producer = None
    try:
        config = {"bootstrap.servers": "localhost:35955", "acks": "all"}
        topic = "github_firehose"
        producer = Producer(config)
        with (
            EventSource(
                "http://github-firehose.libraries.io/events", timeout=30
            ) as event_source,
        ):
            for event in event_source:
                value = json.loads(event.data)
                key = value["id"]
                producer.produce(
                    topic, key=key, value=json.dumps(value), callback=delivery_callback
                )
                logging.info(pformat(json.dumps(value)))
            # producer.poll(10000)
    except:
        raise
    finally:
        if producer is not None:
            producer.flush()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
