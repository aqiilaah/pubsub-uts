# Pub-Sub Log Aggregator

## Deskripsi
Proyek ini adalah sistem Pub-Sub berbasis Python (FastAPI/Flask + asyncio) untuk menerima log/event dari publisher, memprosesnya melalui subscriber/consumer yang idempotent, serta melakukan deduplication untuk mencegah pemrosesan duplikat.  

---

## Asumsi
- Setiap event memiliki format JSON sebagai berikut:

```json
{
  "topic": "string",
  "event_id": "string-unik",
  "timestamp": "ISO8601",
  "source": "string",
  "payload": {...}
}
```

# Aktifkan virtual environment
python -m venv venv
- source venv/bin/activate   # Linux/macOS
- venv\Scripts\activate      # Windows

# Install dependencies
pip install -r requirements.txt

# Jalankan server
uvicorn src.main:app --host localhost --port 8080 --reload

# Build Docker image
docker build -t uts-aggregator .

# Jalankan container
docker run -d -p 8000:8000 uts-aggregator

# Endpoints
- POST /publish
- GET /stats
- GET /events?topic=user_signup

# Demo 
https://youtu.be/ZCDAQE4PtM8?si=ybiweJ8UKNeCjo9i
