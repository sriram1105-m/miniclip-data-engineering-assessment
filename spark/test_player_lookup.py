
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, "/opt/spark/work-dir/data_quality")

from player_lookup import PlayerLookup


with tempfile.TemporaryDirectory() as directory:
    database_path = Path(directory) / "players.db"

    lookup = PlayerLookup(database_path)

    try:
        lookup.update_player("player_0001", "Australia", 100)

        assert lookup.get_country("player_0001") == "Australia"
        print("PASS: Insert and retrieve player country")

        lookup.update_player("player_0001", "Germany", 90)

        assert lookup.get_country("player_0001") == "Australia"
        print("PASS: Older Init cannot overwrite newer Init")

        lookup.update_player("player_0001", "India", 110)

        assert lookup.get_country("player_0001") == "India"
        print("PASS: Newer Init updates player country")

        assert lookup.get_country("unknown_player") is None
        print("PASS: Unknown player returns None")

    finally:
        lookup.close()

    reopened_lookup = PlayerLookup(database_path)

    try:
        assert reopened_lookup.get_country("player_0001") == "India"
        print("PASS: Country persists after reopening database")

    finally:
        reopened_lookup.close()

print("\nPASS: All 5 player lookup tests passed")
