
# Coding Test 2 — Distributed Task Processing with State Management (Go)

Service ini terdiri dari **producer** dan **processor (consumer/worker pool)** yang terhubung ke RabbitMQ dan Redis.

## Jalankan dependency dengan Docker
```bash
docker compose up -d
```

## Jalankan aplikasi
```bash
cd golang-test-two
go mod tidy
go run .
```

### Variabel lingkungan
- `RABBITMQ_URL` (default: `amqp://guest:guest@localhost:5672/`)
- `REDIS_URL` (default: `localhost:6379`)
- `NUM_PRODUCER_REQUESTS` (default: `10`)
- `PUBLISH_INTERVAL` (default: `500ms`)

### Alur
1. Producer mem-publish `ReportRequest` durable ke queue `report_requests`.
2. Processor:
   - `Consume` dengan `autoAck=false` (manual ack).
   - Set status `PENDING` di Redis.
   - Distribusikan tugas ke worker (`NUM_WORKERS` via konstanta di kode).
   - Worker set `IN_PROGRESS`, menjalankan `simulateReportGeneration` (timeout per task), dan set `COMPLETED/FAILED` plus menyimpan hasil lengkap.
   - `resultAckHandler` meng-ack pesan final berdasarkan `ReportResult`.

### Observasi hasil
- Cek status terkini:
  ```bash
  redis-cli GET report:status:<request_id>
  ```
- Cek hasil akhir:
  ```bash
  redis-cli GET report:data:<request_id>
  ```
