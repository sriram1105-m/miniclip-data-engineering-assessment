
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, "/opt/spark/work-dir/data_quality")

from player_lookup import PlayerLookup
from enrichment import enrich_event


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

        result = enrich_event(purchase, lookup)

        assert not result["resolved"]
        assert result["missing_users"] == ["late_player"]
        print("PASS: Purchase unresolved before Init")

        init = {
            "event-type": "init",
            "time": 100,
            "user-id": "late_player",
            "country": "Germany",
            "platform": "ANDROID"
        }

        init_result = enrich_event(init, lookup)
        assert init_result["resolved"]
        print("PASS: Late Init updates lookup")

        recovered = enrich_event(purchase, lookup)

        assert recovered["resolved"]
        assert recovered["event"]["country"] == "Germany"
        print("PASS: Purchase resolves after Init")

    finally:
        lookup.close()

print("\nPASS: All 3 late Init tests passed")
