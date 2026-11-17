
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, "/opt/spark/work-dir/data_quality")

from player_lookup import PlayerLookup
from enrichment import enrich_event

events_path = Path("/opt/spark/sample_output/events.jsonl")

with events_path.open(encoding="utf-8") as file:
    events = [json.loads(line) for line in file]

with tempfile.TemporaryDirectory() as directory:
    lookup = PlayerLookup(Path(directory) / "players.db")

    try:
        # Register all Init events.
        for event in events:
            if event["event-type"] == "init":
                result = enrich_event(event, lookup)
                assert result["resolved"]

        print("PASS: Init events populate player-country lookup")

        # Verify Purchase enrichment.
        purchase = next(
            event for event in events
            if event["event-type"] == "in-app-purchase"
        )

        result = enrich_event(purchase, lookup)

        assert result["resolved"]
        assert result["event"]["country"] == "Australia"

        print("PASS: Purchase enriched with Australia")

        # Verify both countries in a Match.
        match = next(
            event for event in events
            if event["event-type"] == "match"
        )

        result = enrich_event(match, lookup)

        assert result["resolved"]
        assert result["event"]["user-a-country"] == "Australia"
        assert result["event"]["user-b-country"] == "United Kingdom"
        assert result["event"]["country"] == "Australia"

        print("PASS: Match enriched with both player countries")

        # Verify unresolved Purchase handling.
        unknown_purchase = dict(purchase)
        unknown_purchase["user-id"] = "unknown_player"

        result = enrich_event(unknown_purchase, lookup)

        assert not result["resolved"]
        assert result["missing_users"] == ["unknown_player"]

        print("PASS: Unknown Purchase player detected")

        # Verify unresolved Match handling.
        unknown_match = dict(match)
        unknown_match["user-b"] = "unknown_player"

        result = enrich_event(unknown_match, lookup)

        assert not result["resolved"]
        assert result["missing_users"] == ["unknown_player"]

        print("PASS: Unknown Match player detected")

    finally:
        lookup.close()

print("\nPASS: All 5 enrichment tests passed")
