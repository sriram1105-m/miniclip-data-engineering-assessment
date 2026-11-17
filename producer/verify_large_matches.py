
import json
import sys
from collections import Counter

events = [
    json.loads(line)
    for line in sys.stdin
    if line.strip()
]

matches = [
    event
    for event in events
    if event["event-type"] == "match"
]

countries = Counter(
    event.get("country", "MISSING")
    for event in matches
)

print("Match events:", len(matches))
print("Matches by country:")

for country, count in sorted(countries.items()):
    print(f"{country}: {count}")

assert len(events) == 1000, "Incorrect total event count"
assert len(matches) == 200, "Incorrect match count"
assert "MISSING" not in countries, "Missing country enrichment"
assert sum(countries.values()) == 200, "Match count mismatch"

print("PASS: Large-scale match verification")
