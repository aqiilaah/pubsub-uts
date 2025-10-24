import asyncio
from contextlib import asynccontextmanager
from pydantic import BaseModel
from fastapi import FastAPI, Body, HTTPException, Query
from typing import List, Union, Dict, Any

from src.consumer import (
    queue, 
    dstore, 
    stats, 
    consumer_worker, 
    get_stats, 
    get_events
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inisialisasi koneksi database
    await dstore.init()
    
    # Mulai consumer task di background
    task = asyncio.create_task(consumer_worker(queue, dstore))
    
    yield
    
    # Berhenti saat aplikasi shutdown
    task.cancel()
    await dstore.close()

app = FastAPI(lifespan=lifespan)

EventPayload = Dict[str, Any]

class Event(BaseModel):
    topic: str
    event_id: str
    source: str
    payload: EventPayload

@app.post("/publish", status_code=200)
async def publish_event(
    events: Union[Event, List[Event]] = Body(...)
):
    if isinstance(events, list):
        event_list = events
    else:
        event_list = [events]

    for event in event_list:
        try:
            # Hanya logika cepat: update received dan masukkan queue
            stats["received"] += 1
            await queue.put(event.model_dump())
        except Exception:
            # Seharusnya tidak pernah gagal, tapi untuk keamanan
            pass 
            
    return {"accepted": len(event_list)}

@app.get("/stats")
async def get_app_stats():
    return get_stats()

@app.get("/events")
async def get_events_by_topic(topic: str = Query(...)):
    events = await get_events(topic)
    return events