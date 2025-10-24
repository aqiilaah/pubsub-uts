import pytest
import asyncio

# Decorator ini WAJIB untuk semua tes async
@pytest.mark.asyncio
async def test_1_publish_single_unique(api_client):

    print("Running test_1_publish_single_unique")
    event_a = {"topic": "robot", "event_id": "event-A", "timestamp": "2025-01-01T00:00:00Z", "source": "pytest", "payload": {}}

    resp = await api_client.post("/publish", json=event_a)
    assert resp.status_code == 200
    assert resp.json() == {"accepted": 1}

    # Beri waktu 1 detik untuk consumer memproses dari queue
    await asyncio.sleep(1.0) 

    stats_resp = await api_client.get("/stats")
    assert stats_resp.status_code == 200
    stats = stats_resp.json()

    assert stats["received"] == 1
    assert stats["unique"] == 1
    assert stats["duplicate_dropped"] == 0

@pytest.mark.asyncio
async def test_2_deduplication(api_client):

    print("Running test_2_deduplication")
    event_a = {"topic": "robot", "event_id": "event-A", "timestamp": "2025-01-01T00:00:00Z", "source": "pytest", "payload": {}}

    # Kirim 1
    resp1 = await api_client.post("/publish", json=event_a)
    assert resp1.status_code == 200

    # Kirim 2 (Duplikat)
    resp2 = await api_client.post("/publish", json=event_a)
    assert resp2.status_code == 200

    await asyncio.sleep(1.0) # Tunggu consumer

    stats_resp = await api_client.get("/stats")
    assert stats_resp.status_code == 200
    stats = stats_resp.json()

    assert stats["received"] == 2
    assert stats["unique"] == 1
    assert stats["duplicate_dropped"] == 1

@pytest.mark.asyncio
async def test_3_batch_publish_and_dedup(api_client):

    print("Running test_3_batch_publish_and_dedup")
    event_a = {"topic": "batch", "event_id": "event-A", "timestamp": "2025-01-01T00:00:00Z", "source": "pytest", "payload": {}}
    event_b = {"topic": "batch", "event_id": "event-B", "timestamp": "2025-01-01T00:01:00Z", "source": "pytest", "payload": {}}
    
    batch = [event_a, event_b, event_a]

    resp = await api_client.post("/publish", json=batch)
    assert resp.status_code == 200
    assert resp.json() == {"accepted": 3}

    await asyncio.sleep(1.0) # Tunggu consumer

    stats_resp = await api_client.get("/stats")
    assert stats_resp.status_code == 200
    stats = stats_resp.json()

    assert stats["received"] == 3
    assert stats["unique"] == 2
    assert stats["duplicate_dropped"] == 1

@pytest.mark.asyncio
async def test_4_get_events_consistency(api_client):

    print("Running test_4_get_events_consistency")
    event_a = {"topic": "batch", "event_id": "event-A", "timestamp": "2025-01-01T00:00:00Z", "source": "pytest", "payload": {}}
    event_b = {"topic": "batch", "event_id": "event-B", "timestamp": "2025-01-01T00:01:00Z", "source": "pytest", "payload": {}}
    
    await api_client.post("/publish", json=[event_a, event_b, event_a])
    
    await asyncio.sleep(1.0) # Tunggu consumer

    events_resp = await api_client.get("/events?topic=batch")
    assert events_resp.status_code == 200
    events = events_resp.json()

    assert len(events) == 2
    
    # Cek apakah event_id A dan B ada di dalam list
    event_ids_received = {e["event_id"] for e in events}
    assert event_ids_received == {"event-A", "event-B"}

@pytest.mark.asyncio
async def test_5_schema_validation_error(api_client):

    print("Running test_5_schema_validation_error")
    bad_event = {"topic": "robot", "source": "pytest", "payload": {}} # 'event_id' hilang

    resp = await api_client.post("/publish", json=bad_event)

    assert resp.status_code == 422 # 422 Unprocessable Entity