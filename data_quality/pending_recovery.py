
import json

from enrichment import enrich_event


def recover_pending_events(player_lookup):
    """
    Retry pending events using the latest player-country lookup.

    Successfully enriched events are returned to the caller.
    Unresolved events remain stored for future retries.
    """

    recovered_events = []

    for event_id, event_json, created_at in (
        player_lookup.get_pending_events()
    ):
        event = json.loads(event_json)

        result = enrich_event(event, player_lookup)

        if result["resolved"]:
            recovered_events.append({
                "event_id": event_id,
                "event": result["event"]
            })

    return recovered_events


def acknowledge_recovered_event(player_lookup, event_id):
    """
    Delete a recovered event only after downstream processing
    has successfully completed.
    """

    player_lookup.delete_pending_event(event_id)
