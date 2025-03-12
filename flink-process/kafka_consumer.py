import json
import pprint
from datetime import datetime

from typing import Iterable

from pyflink.common import Duration

from pyflink.common import Time, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import ProcessAllWindowFunction
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.window import (
    EventTimeTrigger,
    TimeWindow,
    TumblingEventTimeWindows,
)


# Not used: quick lambda function is used instead
def extract_display_login(event):
    """Exracts display login from a Github firehose event"""
    try:
        # First, parse the JSON string
        # print(event)
        parsed_event = json.loads(event)

        # Check if 'actor' and 'display_login' exist
        if "actor" in parsed_event and "display_login" in parsed_event["actor"]:
            # print(parsed_event["actor"]["display_login"])
            return parsed_event["actor"]["display_login"]
        else:
            # Log the event structure if the expected keys are missing
            print(f"Missing 'actor' or 'display_login' in event: {parsed_event}")
            return "unknown"
    except json.JSONDecodeError:
        print(f"Failed to parse JSON: {event}")
        return "unknown"
    except KeyError as e:
        print(f"Key error: {e}")
        print(f"Event structure: {event}")
        return "unknown"
    except Exception as e:
        print(f"Unexpected error extracting display_login: {e}")
        return "unknown"


def make_pretty(value: str) -> str | None:
    return pprint.pformat(value)


# Not used: Kafka source assigns its own timestamp
class GithubEventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        """Create a timestamp assigner and watermark generator"""

        created_at = json.loads(value)["created_at"]
        return created_at


class AllWindowFunction(ProcessAllWindowFunction):
    def process(
        self, context: ProcessAllWindowFunction.Context, elements: Iterable[tuple]
    ) -> Iterable[object]:
        window: TimeWindow = context.window()
        window_start = window.start
        window_end = window.end
        all_events = {}
        for input in elements:
            key, value = input
            if key in all_events:
                all_events[key] += value
            else:
                all_events[key] = value

        result = {
            "top_results": sorted(
                all_events.items(), key=lambda item: item[1], reverse=True
            )[0:4],
            "window_start": window_start,
            "window_end": window_end,
            "window_start_readable": datetime.fromtimestamp(
                window_start / 1000.0
            ).strftime("%Y-%m-%d %H:%M:%S"),
            "window_end_readable": datetime.fromtimestamp(window_end / 1000.0).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }

        yield result


def main() -> None:
    # Create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # Get current directory
    current_dir_list = __file__.split("/")[:-1]
    current_dir = "/".join(current_dir_list)

    # Adding the jar to the flink streaming environment
    env.add_jars(f"file://{current_dir}/flink-sql-connector-kafka-3.1.0-1.18.jar")

    properties = {
        "bootstrap.servers": "localhost:19092",
        "group.id": "iot-sensors",
    }

    earliest = False
    offset = (
        KafkaOffsetsInitializer.earliest()
        if earliest
        else KafkaOffsetsInitializer.latest()
    )

    # Create a Kafka Source
    # NOTE: FlinkKafkaConsumer class is deprecated
    kafka_source = (
        KafkaSource.builder()
        .set_topics("github_events")
        .set_properties(properties)
        .set_starting_offsets(
            KafkaOffsetsInitializer.latest()
        )  # This step is important. Having the latest KafkaOffsetsInitializer takes the latest event. See [additional options](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/kafka/#starting-offset)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Create a DataStream from the Kafka source and assign watermarks
    data_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(10)
        ),  # No timestamp assigner as Kafka source automatically assigns timestamps
        source_name="Github events topic",
    )

    # Print line for readablity in the console
    print("start reading data from kafka")

    # The display login will be the key for the stream.
    # We eventually want to aggregate, so we will assign tuples with a value of 1.
    # This represents the number of events (at this point, one)
    mapped_to_user = data_stream.map(
        lambda x: (json.loads(x)["actor"]["display_login"], 1)
    )

    window_size_seconds = 10
    print(
        f"Printing top 5 Github event publishers in the last {window_size_seconds} seconds."
    )
    top_results = (
        (
            mapped_to_user.window_all(
                TumblingEventTimeWindows.of(Time.seconds(window_size_seconds))
            )
        )
        .trigger(EventTimeTrigger.create())
        .allowed_lateness(0)
        .process(AllWindowFunction())
        .print()
    )

    env.execute("Github events consumer")


if __name__ == "__main__":
    main()
