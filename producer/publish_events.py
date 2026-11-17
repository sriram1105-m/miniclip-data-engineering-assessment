
import argparse
import json
from pathlib import Path

from kafka import KafkaProducer


def publish_events(input_path, topic, bootstrap_servers, delay):
    """Publish generated game events to Kafka in their original order."""

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda event: json.dumps(event).encode("utf-8"),
        acks="all",
        retries=3,
        max_in_flight_requests_per_connection=1,
    )

    count = 0

    try:
        with Path(input_path).open("r", encoding="utf-8") as file:
            for line in file:
                if not line.strip():
                    continue

                event = json.loads(line)

                # A constant key routes all events to the same partition,
                # preserving their order in this local demonstration.
                producer.send(
                    topic,
                    key="game-events",
                    value=event,
                ).get(timeout=30)

                count += 1
                print(f"Published event {count}: {event['event-type']}")

                if delay > 0:
                    import time
                    time.sleep(delay)

        producer.flush(timeout=30)

    finally:
        producer.close(timeout=30)

    print(f"\nSuccessfully published {count} events to '{topic}'.")


def main():
    parser = argparse.ArgumentParser(
        description="Publish simulated Miniclip events to Kafka."
    )
    parser.add_argument("--input", default="sample_output/events.jsonl")
    parser.add_argument("--topic", default="raw-events")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Seconds between published events.",
    )

    args = parser.parse_args()

    if args.delay < 0:
        parser.error("--delay cannot be negative.")

    publish_events(
        args.input,
        args.topic,
        args.bootstrap_servers,
        args.delay,
    )


if __name__ == "__main__":
    main()
