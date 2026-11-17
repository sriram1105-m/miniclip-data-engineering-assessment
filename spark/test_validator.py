
import json
import sys

sys.path.insert(0, "/opt/spark/work-dir/data_quality")

from validator import EventValidator


validator = EventValidator()


def check(name, event, expected_valid):
    raw_event = (
        event if isinstance(event, str)
        else json.dumps(event)
    )

    result = validator.validate(raw_event)

    assert result["valid"] == expected_valid, (
        f"{name}: expected valid={expected_valid}, "
        f"got {result}"
    )

    print(f"PASS: {name}")

    if not result["valid"]:
        print(f"  Errors: {result['errors']}")


valid_init = {
    "event-type": "init",
    "time": 1763200000,
    "user-id": "player-101",
    "country": "IN",
    "platform": "android"
}

valid_purchase = {
    "event-type": "in-app-purchase",
    "time": 1763200000,
    "user-id": "player-101",
    "product-id": "coins-100",
    "purchase_value": 4.99
}

valid_match = {
    "event-type": "match",
    "time": 1763200000,
    "user-a": "player-101",
    "user-b": "player-102",
    "user-a-postmatch-info": {
        "coin-balance-after-match": 100,
        "level-after-match": 5,
        "device": "Samsung",
        "platform": "android"
    },
    "winner": "player-101",
    "game-tier": 5,
    "duration": 120
}


check("Valid Init", valid_init, True)
check("Valid Purchase", valid_purchase, True)
check("Valid Match", valid_match, True)

check("Malformed JSON", '{"event-type":', False)

check(
    "Missing required country",
    {key: value for key, value in valid_init.items()
     if key != "country"},
    False
)

check(
    "Negative purchase value",
    {**valid_purchase, "purchase_value": -5},
    False
)

check(
    "Unexpected Init field",
    {**valid_init, "unknown-field": "test"},
    False
)

check(
    "Invalid nested Match platform",
    {
        **valid_match,
        "user-a-postmatch-info": {
            **valid_match["user-a-postmatch-info"],
            "platform": ""
        }
    },
    False
)

check(
    "Unsupported event type",
    {"event-type": "unknown", "time": 1763200000},
    False
)

print("\nPASS: All 9 validator tests passed")
