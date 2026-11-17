
import sys
import json
from decimal import Decimal
from collections import defaultdict

events = [
    json.loads(line)
    for line in sys.stdin
    if line.strip()
]

totals = defaultdict(lambda: {
    "purchases": 0,
    "revenue": Decimal("0.00")
})

for event in events:
    if event["event-type"] != "in-app-purchase":
        continue

    country = event.get("country", "MISSING")
    totals[country]["purchases"] += 1
    totals[country]["revenue"] += Decimal(str(event["purchase_value"]))

print("Country | Purchases | Revenue")

for country, values in sorted(totals.items()):
    print(
        f"{country} | "
        f"{values['purchases']} | "
        f"{values['revenue']:.2f}"
    )

total_purchases = sum(v["purchases"] for v in totals.values())
total_revenue = sum(
    (v["revenue"] for v in totals.values()),
    Decimal("0.00")
)

print(f"\nTotal purchases: {total_purchases}")
print(f"Total revenue: {total_revenue:.2f}")

assert len(events) == 1000, "Incorrect total event count"
assert total_purchases == 400, "Incorrect purchase count"
assert total_revenue == Decimal("9632.10"), "Revenue mismatch"
assert "MISSING" not in totals, "Missing country enrichment"

print("PASS: Large-scale revenue verification")
