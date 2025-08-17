package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// ItemDetails merepresentasikan informasi rinci untuk sebuah item.
type ItemDetails struct {
	ID          string
	Name        string
	Description string
	Price       float64
}

// simulateFetchItemDetails mensimulasikan pemanggilan API eksternal untuk mengambil detail item.
// Fungsi ini memperkenalkan penundaan acak dan kesalahan sesekali.
// Fungsi ini menghormati context untuk pembatalan.
// simulateFetchItemDetails mensyaratkan RNG lokal sehingga aman dipakai
// konkuren dari beberapa goroutine. Ini menghindari pemanggilan
// rand.Seed yang deprecated dan race pada generator global.
func simulateFetchItemDetails(ctx context.Context, itemID string, rng *rand.Rand) (*ItemDetails, error) {
	// Mensimulasikan latensi jaringan
	delay := time.Duration(500+rng.Intn(1500)) * time.Millisecond // 0.5s to 2s
	select {
	case <-time.After(delay):
		// Lanjutkan
	case <-ctx.Done():
		log.Printf("Context cancelled for item %s, aborting fetch.", itemID)
		return nil, ctx.Err() // Context cancelled
	}

	// Mensimulasikan error API sesekali
	if rng.Intn(100) < 15 {
		log.Printf("Simulated API error for item %s", itemID)
		return nil, fmt.Errorf("simulated API error for item %s: service unavailable", itemID)
	}

	details := &ItemDetails{
		ID:          itemID,
		Name:        fmt.Sprintf("Product %s", itemID),
		Description: fmt.Sprintf("Detailed description for product %s.", itemID),
		Price:       rng.Float64() * 100,
	}
	log.Printf("Successfully fetched details for item %s", itemID)
	return details, nil
}

// FetchAndAggregate mengambil ItemDetails secara Concurrently untuk itemIDs yang diberikan dengan batas konkruensi,
// timeout per-item, dan pengumpulan semua error. Fungsi ini menghormati ctx global untuk pembatalan.
// FetchAndAggregate menerima parameter seed agar setiap goroutine dapat
// membuat RNG lokal yang deterministik (seed + index). Dengan cara ini
// kita menghindari penggunaan generator global yang tidak aman untuk
// dipakai Concurrently.
func FetchAndAggregate(
	ctx context.Context,
	itemIDs []string,
	maxConcurrent int,
	perItemTimeout time.Duration,
	seed int64,
) (map[string]ItemDetails, []error) {

	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}

	results := make(map[string]ItemDetails)
	var resultsMu sync.Mutex
	var errsMu sync.Mutex
	var errs []error

	// semaphore untuk membatasi konkruensi
	sem := make(chan struct{}, maxConcurrent)

	var wg sync.WaitGroup
	wg.Add(len(itemIDs))

	for i, id := range itemIDs {
		id := id // capture
		i := i
		go func() {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
				// slot diperoleh
			case <-ctx.Done():
				// context global dibatalkan sebelum memulai
				errsMu.Lock()
				errs = append(errs, fmt.Errorf("skipped %s due to global cancellation: %w", id, ctx.Err()))
				errsMu.Unlock()
				return
			}
			defer func() { <-sem }()

			// Context timeout per-item yang diturunkan dari context global
			itemCtx, cancel := context.WithTimeout(ctx, perItemTimeout)
			defer cancel()

			// Buat RNG lokal untuk goroutine ini. Gunakan seed yang
			// diteruskan ditambah index agar aliran angka acak berbeda
			// antar-goroutine namun tetap deterministik bila seed tetap.
			localRng := rand.New(rand.NewSource(seed + int64(i)))

			d, err := simulateFetchItemDetails(itemCtx, id, localRng)
			if err != nil {
				errsMu.Lock()
				errs = append(errs, fmt.Errorf("item %s: %w", id, err))
				errsMu.Unlock()
				return
			}

			resultsMu.Lock()
			results[d.ID] = *d
			resultsMu.Unlock()
		}()
	}

	// Tunggu hingga semua goroutine selesai atau ada pembatalan global
	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		// completed
	case <-ctx.Done():
		// wait for goroutines to exit gracefully
		<-doneCh
	}

	return results, errs
}

func main() {
	// Tidak menggunakan rand.Seed global; setiap goroutine membuat RNG
	// lokal yang diberi seed agar aman untuk konkruensi.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	itemIDs := []string{"A1", "B2", "C3", "D4", "E5", "F6", "G7", "H8"}
	maxConcurrent := 3
	perItemTimeout := 1500 * time.Millisecond

	// Gunakan seed waktu saat runtime agar hasil acak berbeda tiap run.
	// Jika Anda ingin output yang sama setiap run untuk debugging, ganti
	// dengan angka tetap, mis. seed := int64(42).
	seed := time.Now().UnixNano()
	res, errs := FetchAndAggregate(ctx, itemIDs, maxConcurrent, perItemTimeout, seed)
	fmt.Println("=== Aggregated Results ===")
	for id, d := range res {
		fmt.Printf("%s -> %s | %.2f\n", id, d.Name, d.Price)
	}

	fmt.Println("=== Errors ===")
	for _, e := range errs {
		fmt.Println("-", e)
	}
}
