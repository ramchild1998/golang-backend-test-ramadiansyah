package main

import (
	"context"
	"testing"
	"time"
)

// TestFetchAndAggregate_HappyPath memastikan FetchAndAggregate mengumpulkan
// detail untuk item yang responsif dalam batas timeout.
func TestFetchAndAggregate_HappyPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	itemIDs := []string{"X1", "Y2", "Z3"}
	maxConcurrent := 2
	perItemTimeout := 2 * time.Second
	seed := int64(99)

	res, errs := FetchAndAggregate(ctx, itemIDs, maxConcurrent, perItemTimeout, seed)

	if len(errs) > 0 {
		t.Fatalf("expected no errors in happy path, got: %v", errs)
	}

	if len(res) != len(itemIDs) {
		t.Fatalf("expected results for %d items, got %d", len(itemIDs), len(res))
	}
}

// TestFetchAndAggregate_Timeout memastikan item yang lambat menghasilkan error
// dan fungsi tetap mengembalikan result lain.
func TestFetchAndAggregate_Timeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// gunakan banyak item agar beberapa mungkin melewati timeout
	itemIDs := []string{"A1", "B2", "C3", "D4", "E5"}
	maxConcurrent := 3
	perItemTimeout := 500 * time.Millisecond // kecil sehingga beberapa akan timeout
	seed := int64(123)

	res, errs := FetchAndAggregate(ctx, itemIDs, maxConcurrent, perItemTimeout, seed)

	// Karena perItemTimeout kecil, kita mengharapkan setidaknya satu error.
	if len(errs) == 0 {
		t.Fatalf("expected some errors due to per-item timeout")
	}

	// Pastikan fungsi mengembalikan tanpa panic; hasil sukses bisa 0 tergantung RNG.
	if len(res)+len(errs) != len(itemIDs) {
		t.Fatalf("expected total outcomes equal to items; got results=%d errors=%d", len(res), len(errs))
	}
}
