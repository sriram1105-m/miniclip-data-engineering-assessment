
import sys
import json
import tempfile
from pathlib import Path

sys.path.insert(0, "/opt/spark/work-dir/data_quality")

from player_lookup import PlayerLookup


with tempfile.TemporaryDirectory() as directory:
    database_path = Path(directory) / "players.db"

    lookup = PlayerLookup(database_path)

    event = {
        "event-type": "in-app-purchase",
        "time": 200,
        "user-id": "late_player",
        "product-id": "coins_100",
        "purchase_value": 9.99
    }

    event_json = json.dumps(event)

    try:
        lookup.save_pending_event(
            "raw-events:0:100",
            event_json,
            200
        )

        pending = lookup.get_pending_events()

        assert len(pending) == 1
        assert json.loads(pending[0][1]) == event

        print("PASS: Pending event stored and retrieved")

        lookup.save_pending_event(
            "raw-events:0:100",
            event_json,
            200
        )

        assert len(lookup.get_pending_events()) == 1

        print("PASS: Duplicate event ID prevented")

    finally:
        lookup.close()

    reopened = PlayerLookup(database_path)

    try:
        pending = reopened.get_pending_events()

        assert len(pending) == 1

        print("PASS: Pending event survives database reopening")

        reopened.delete_pending_event("raw-events:0:100")

        assert len(reopened.get_pending_events()) == 0

        print("PASS: Pending event deleted successfully")

    finally:
        reopened.close()

print("\nPASS: All 4 pending-event tests passed")
