
import argparse
import json
import random
import time
from pathlib import Path


COUNTRIES = [
    "India",
    "United Kingdom",
    "Germany",
    "Portugal",
    "Spain",
    "Australia",
]

PLATFORMS = ["Android", "iOS"]

DEVICES = [
    "Samsung Galaxy S24",
    "iPhone 15",
    "Google Pixel 8",
    "OnePlus 12",
]

PRODUCTS = [
    "coins_100",
    "coins_500",
    "premium_pack",
    "starter_bundle",
]


def generate_player(player_number, rng, event_time):
    """Create one player's initial event and retain their attributes."""

    user_id = f"player_{player_number:04d}"
    country = rng.choice(COUNTRIES)
    platform = rng.choice(PLATFORMS)
    device = rng.choice(DEVICES)

    init_event = {
        "event-type": "init",
        "time": event_time,
        "user-id": user_id,
        "country": country,
        "platform": platform,
    }

    player = {
        "user_id": user_id,
        "country": country,
        "platform": platform,
        "device": device,
        "coin_balance": rng.randint(100, 5000),
        "level": rng.randint(1, 50),
    }

    return init_event, player


def generate_match(player_a, player_b, rng, event_time):
    """Create a match event between two initialized players."""

    winner = rng.choice([
        player_a["user_id"],
        player_b["user_id"],
    ])

    def postmatch_info(player):
        return {
            "coin-balance-after-match": player["coin_balance"],
            "level-after-match": player["level"],
            "device": player["device"],
            "platform": player["platform"],
        }

    return {
        "event-type": "match",
        "time": event_time,
        "user-a": player_a["user_id"],
        "user-b": player_b["user_id"],
        "user-a-postmatch-info": postmatch_info(player_a),
        "user-b-postmatch-info": postmatch_info(player_b),
        "winner": winner,
        "game-tier": rng.randint(5, 10),
        "duration": rng.randint(60, 1800),
    }


def generate_purchase(player, rng, event_time):
    """Create an in-app purchase event for an initialized player."""

    return {
        "event-type": "in-app-purchase",
        "time": event_time,
        "purchase_value": round(
            rng.uniform(0.99, 49.99), 2
        ),
        "user-id": player["user_id"],
        "product-id": rng.choice(PRODUCTS),
    }


def generate_events(number_of_players, seed):
    """Generate Init events first, followed by matches and purchases."""

    rng = random.Random(seed)
    current_time = int(time.time())
    event_sequence = 0

    players = []
    events = []

    # Generate an Init event for every player first.
    for player_number in range(1, number_of_players + 1):
        init_event, player = generate_player(
            player_number,
            rng,
            current_time + event_sequence,
        )

        players.append(player)
        events.append(init_event)
        event_sequence += 1

    # Generate matches between pairs of initialized players.
    for index in range(0, len(players) - 1, 2):
        match_event = generate_match(
            players[index],
            players[index + 1],
            rng,
            current_time + event_sequence,
        )

        events.append(match_event)
        event_sequence += 1

    # Generate one purchase event per player.
    for player in players:
        purchase_event = generate_purchase(
            player,
            rng,
            current_time + event_sequence,
        )

        events.append(purchase_event)
        event_sequence += 1

    return events


def save_events(events, output_path):
    """Write events in JSON Lines format."""

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", encoding="utf-8") as file:
        for event in events:
            file.write(json.dumps(event) + "\n")

    return output_file


def main():
    parser = argparse.ArgumentParser(
        description="Generate Miniclip game events."
    )

    parser.add_argument(
        "--players",
        type=int,
        default=10,
        help="Number of players to generate.",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible event data.",
    )

    parser.add_argument(
        "--output",
        default="sample_output/events.jsonl",
        help="Output JSONL file path.",
    )

    args = parser.parse_args()

    if args.players < 2:
        parser.error("At least two players are required.")

    events = generate_events(args.players, args.seed)
    output_file = save_events(events, args.output)

    event_counts = {}
    for event in events:
        event_type = event["event-type"]
        event_counts[event_type] = (
            event_counts.get(event_type, 0) + 1
        )

    print("Event generation completed successfully.")
    print(f"Players: {args.players}")
    print(f"Total events: {len(events)}")
    print(f"Event breakdown: {event_counts}")
    print(f"Output file: {output_file}")


if __name__ == "__main__":
    main()
