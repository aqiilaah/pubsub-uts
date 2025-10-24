import aiosqlite
from pathlib import Path
import json

class DedupStore:
    def __init__(self, db_path="dedup.db"):
        self.db_path = Path(db_path)
        self.conn = None

    async def init(self):
        # Metode ini sekarang ADA dan ASYNC
        self.conn = await aiosqlite.connect(self.db_path)
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT,
                topic TEXT,
                source TEXT,
                payload TEXT,
                received_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (event_id, topic)
            )
            """
        )
        await self.conn.commit()

    async def close(self):
        # Metode ini sekarang ADA dan ASYNC
        if self.conn:
            await self.conn.close()
            self.conn = None

    async def check_and_store(self, event: dict):
        # Metode ini COCOK dengan yang dipanggil consumer_worker
        if not self.conn:
            await self.init() 
            
        try:
            await self.conn.execute(
                "INSERT INTO events (event_id, topic, source, payload) VALUES (?, ?, ?, ?)",
                (
                    event.get("event_id"),
                    event.get("topic"),
                    event.get("source"),
                    json.dumps(event.get("payload", {})),
                ),
            )
            await self.conn.commit()
            return True # Unik
        except aiosqlite.IntegrityError:
            return False # Duplikat

    async def get_events_by_topic(self, topic: str):
        if not self.conn:
            await self.init()

        cursor = await self.conn.execute(
            "SELECT event_id, source, payload FROM events WHERE topic = ?", (topic,)
        )
        rows = await cursor.fetchall()
        await cursor.close()
        
        events = []
        for row in rows:
            try:
                payload = json.loads(row[2])
            except (json.JSONDecodeError, TypeError):
                payload = {}
                
            events.append({
                "event_id": row[0],
                "source": row[1],
                "payload": payload
            })
        return events