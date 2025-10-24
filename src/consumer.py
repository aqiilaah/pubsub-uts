import asyncio
import logging
from src.dedup_store import DedupStore

logger = logging.getLogger("consumer")

# State global di-instansiasi di sini
queue = asyncio.Queue()
dstore = DedupStore() 
stats = {
    "received": 0,
    "unique": 0,
    "duplicates": 0,
    "topics": set()
}

async def consumer_worker(queue: asyncio.Queue, dstore: DedupStore):
    while True:
        try:
            event = await queue.get()
            
            stats["topics"].add(event.get("topic"))
            
            is_unique = await dstore.check_and_store(event) 
            
            if is_unique:
                stats["unique"] += 1
            else:
                stats["duplicates"] += 1

            queue.task_done()

        except Exception as e:
            logger.error(f"Consumer worker error: {e}", exc_info=True)
            queue.task_done()

def get_stats():
    # Buat salinan state saat ini
    current_stats = stats.copy()
    current_stats["topics"] = list(current_stats["topics"])
    # Hapus uptime_sec jika tidak di-manage
    return current_stats

async def get_events(topic: str):
    return await dstore.get_events_by_topic(topic)