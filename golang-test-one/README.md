
# Coding Test 1 — Concurrent Data Aggregation Service (Go)

## Cara jalan cepat
```bash
cd golang-test-one
go run .
```

## Catatan teknis
- `FetchAndAggregate` menerapkan:
  - Bounded concurrency via *semaphore* (`chan struct{}` kapasitas `maxConcurrent`).
  - Timeout per-item via `context.WithTimeout(ctx, perItemTimeout)`.
  - Penghormatan `ctx` global — bila dibatalkan, goroutine yang aktif akan segera berhenti.
  - Koleksi hasil dan error thread-safe via `sync.Mutex`.
 - Implementasi sekarang tidak menggunakan `rand.Seed` global.
   Sebagai gantinya `FetchAndAggregate` menerima `seed int64` dan setiap
   goroutine membuat RNG lokal (`rand.New(rand.NewSource(seed + i))`), sehingga
   aman dipakai konkuren. Secara default `main` menggunakan `seed := time.Now().UnixNano()` sehingga
   hasil berbeda tiap run. Untuk debug, gunakan seed tetap (mis. `42`).

## Menjalankan test
```bash
cd golang-test-one
go test
```
