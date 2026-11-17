
import sqlite3
from pathlib import Path


class PlayerLookup:
    def __init__(self, database_path):
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(
            parents=True, exist_ok=True
        )

        self.connection = sqlite3.connect(
            str(self.database_path)
        )

        # Store the latest known country for each player.
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS players (
                user_id TEXT PRIMARY KEY,
                country TEXT NOT NULL,
                event_time INTEGER NOT NULL
            )
        """)

        # Store events waiting for missing Init records.
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS pending_events (
                event_id TEXT PRIMARY KEY,
                event_json TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )
        """)

        self.connection.commit()

    def update_player(self, user_id, country, event_time):
        """Keep the country from the latest Init event."""

        self.connection.execute("""
            INSERT INTO players (
                user_id, country, event_time
            )
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                country = excluded.country,
                event_time = excluded.event_time
            WHERE excluded.event_time >= players.event_time
        """, (str(user_id), country, int(event_time)))

        self.connection.commit()

    def get_country(self, user_id):
        """Return the latest known country for a player."""

        cursor = self.connection.execute(
            "SELECT country FROM players WHERE user_id = ?",
            (str(user_id),)
        )

        result = cursor.fetchone()
        return result[0] if result else None

    def save_pending_event(self, event_id, event_json, created_at):
        """Store an unresolved event without creating duplicates."""

        self.connection.execute("""
            INSERT OR IGNORE INTO pending_events (
                event_id, event_json, created_at
            )
            VALUES (?, ?, ?)
        """, (
            str(event_id),
            event_json,
            int(created_at)
        ))

        self.connection.commit()

    def get_pending_events(self):
        """Retrieve pending events in insertion-time order."""

        cursor = self.connection.execute("""
            SELECT event_id, event_json, created_at
            FROM pending_events
            ORDER BY created_at, event_id
        """)

        return cursor.fetchall()

    def delete_pending_event(self, event_id):
        """Remove an event after successful processing."""

        self.connection.execute("""
            DELETE FROM pending_events
            WHERE event_id = ?
        """, (str(event_id),))

        self.connection.commit()

    def close(self):
        """Close the database connection."""
        self.connection.close()
