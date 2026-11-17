
import json
from pathlib import Path

from jsonschema import Draft3Validator


SCHEMA_DIRECTORY = Path(
    "/opt/spark/work-dir/schemas"
)

SCHEMA_FILES = {
    "init": "init.json",
    "match": "match.json",
    "in-app-purchase": "in-app-purchase.json",
}


class EventValidator:
    def __init__(self, schema_directory=SCHEMA_DIRECTORY):
        self.validators = {}

        for event_type, filename in SCHEMA_FILES.items():
            schema_path = Path(schema_directory) / filename

            with schema_path.open(encoding="utf-8") as file:
                schema = json.load(file)

            Draft3Validator.check_schema(schema)

            self.validators[event_type] = Draft3Validator(schema)

    def validate(self, raw_event):
        """Return a validation result without raising on bad events."""

        try:
            event = json.loads(raw_event)
        except (json.JSONDecodeError, TypeError):
            return {
                "valid": False,
                "event": None,
                "errors": ["Malformed JSON"]
            }

        if not isinstance(event, dict):
            return {
                "valid": False,
                "event": None,
                "errors": ["Event must be a JSON object"]
            }

        event_type = event.get("event-type")

        if event_type not in self.validators:
            return {
                "valid": False,
                "event": event,
                "errors": [f"Unsupported event type: {event_type}"]
            }

        validator = self.validators[event_type]

        errors = sorted(
            validator.iter_errors(event),
            key=lambda error: (
                tuple(str(part) for part in error.absolute_path),
                error.message
            )
        )

        if errors:
            return {
                "valid": False,
                "event": event,
                "errors": [
                    {
                        "field": ".".join(
                            str(part) for part in error.absolute_path
                        ) or "$",
                        "message": error.message
                    }
                    for error in errors
                ]
            }

        return {
            "valid": True,
            "event": event,
            "errors": []
        }
