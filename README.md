
# Backend Developer Test (Go) — Ramadiansyah

## Jawaban Essay
Bisa dilihat dari file JAWABAN_ESSAY.md dan di link berikut:
[**text**](https://docs.google.com/document/d/1DvXlCsvS0YWBON-I0GDeLbFDtxqFtt6eIKo6ZpioN7E/edit?usp=sharing)

Repositori ini berisi dua project sesuai instruksi pada soal test:
- `golang-test-one` — **Concurrent Data Aggregation Service**
- `golang-test-two` — **Distributed Task Processing with State Management** (RabbitMQ + Redis)

## Prasyarat
- Go 1.22+
- Docker (untuk RabbitMQ & Redis pada test kedua)

## Cara Menjalankan Test Coding 1 & 2
```bash
# Test 1
cd golang-test-one
go run .

# Test 2 (dependency dulu)
cd ..
docker compose up -d
cd golang-test-two
go run .
```
