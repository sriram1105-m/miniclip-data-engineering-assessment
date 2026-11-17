
def enrich_event(event, player_lookup):
    """Enrich a validated event using known player countries."""

    enriched = dict(event)
    event_type = event["event-type"]

    if event_type == "init":
        player_lookup.update_player(
            event["user-id"],
            event["country"],
            event["time"]
        )

        return {
            "resolved": True,
            "event": enriched,
            "missing_users": []
        }

    if event_type == "in-app-purchase":
        country = player_lookup.get_country(event["user-id"])

        if country is None:
            return {
                "resolved": False,
                "event": enriched,
                "missing_users": [str(event["user-id"])]
            }

        enriched["country"] = country

        return {
            "resolved": True,
            "event": enriched,
            "missing_users": []
        }

    if event_type == "match":
        user_a = event["user-a"]
        user_b = event["user-b"]

        country_a = player_lookup.get_country(user_a)
        country_b = player_lookup.get_country(user_b)

        missing_users = []

        if country_a is None:
            missing_users.append(str(user_a))

        if country_b is None:
            missing_users.append(str(user_b))

        if missing_users:
            return {
                "resolved": False,
                "event": enriched,
                "missing_users": missing_users
            }

        enriched["user-a-country"] = country_a
        enriched["user-b-country"] = country_b
        enriched["country"] = country_a

        return {
            "resolved": True,
            "event": enriched,
            "missing_users": []
        }

    raise ValueError(f"Unsupported event type: {event_type}")
