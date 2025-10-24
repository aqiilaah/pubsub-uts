FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Jalankan FastAPI
CMD ["uvicorn", "src.main:app", "--host", "localhost", "--port", "8080"]
