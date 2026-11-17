
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, "/opt/spark/work-dir/data_quality")

from player_lookup import PlayerLookup
from enrichment import enrich_event
from pending_recovery import (
    recover_pending_events,
    acknowledge_recovered_event
)


with tempfile.TemporaryDirectory() as directory:
    lookup = PlayerLookup(Path(directory) / "players.db")

    try:
        purchase = {
            "event-type": "in-app-purchase",
            "time": 200,
            "user-id": "late_player",
            "product-id": "coins_100",
            "purchase_value": 9.99
        }

        event_id = "raw-events:0:100"

        # Purchase arrives before the player's Init.
        result = enrich_event(purchase, lookup)

        assert not result["resolved"]

        lookup.save_pending_event(
            event_id,
            json.dumps(purchase),
            purchase["time"]
        )

        assert len(lookup.get_pending_events()) == 1
        print("PASS: Unresolved Purchase stored")

        # Recovery attempt before Init must not succeed.
        recovered = recover_pending_events(lookup)

        assert len(recovered) == 0
        assert len(lookup.get_pending_events()) == 1

        print("PASS: Purchase remains pending before Init")

        # The missing Init arrives later.
        init = {
            "event-type": "init",
            "time": 100,
            "user-id": "late_player",
            "country": "Germany",
            "platform": "ANDROID"
        }

        result = enrich_event(init, lookup)

        assert result["resolved"]
        print("PASS: Late Init updates player lookup")

        # Retry pending events.
        recovered = recover_pending_events(lookup)

        assert len(recovered) == 1
        assert recovered[0]["event_id"] == event_id
        assert recovered[0]["event"]["country"] == "Germany"

        print("PASS: Pending Purchase recovered with Germany")

        # Recovery must not delete an event prematurely.
        assert len(lookup.get_pending_events()) == 1

        print("PASS: Recovered event retained until acknowledgement")

        # Simulate successful downstream processing.
        acknowledge_recovered_event(lookup, event_id)

        assert len(lookup.get_pending_events()) == 0

        print("PASS: Acknowledged event removed")

    finally:
        lookup.close()

print("\nPASS: All 6 pending recovery tests passed")
