package subscription

import (
	"sync"
	"testing"
)

func TestManagerRefCounting(t *testing.T) {
	var mu sync.Mutex
	var first, zero []string

	m := NewManager(
		func(s string) error {
			mu.Lock()
			first = append(first, s)
			mu.Unlock()
			return nil
		},
		func(s string) error {
			mu.Lock()
			zero = append(zero, s)
			mu.Unlock()
			return nil
		},
	)

	// subscribe 3x simbol sama -> onFirst hanya sekali
	_ = m.Subscribe("FOREXCOM:XAUUSD")
	_ = m.Subscribe("FOREXCOM:XAUUSD")
	_ = m.Subscribe("FOREXCOM:XAUUSD")

	if len(first) != 1 || first[0] != "FOREXCOM:XAUUSD" {
		t.Fatalf("onFirst dipanggil %d kali: %v", len(first), first)
	}

	// unsubscribe 2x -> masih aktif (ref 1)
	_ = m.Unsubscribe("FOREXCOM:XAUUSD")
	_ = m.Unsubscribe("FOREXCOM:XAUUSD")
	if len(zero) != 0 {
		t.Fatalf("onZero tidak boleh dipanggil, got %v", zero)
	}

	// unsubscribe terakhir -> onZero
	_ = m.Unsubscribe("FOREXCOM:XAUUSD")
	if len(zero) != 1 || zero[0] != "FOREXCOM:XAUUSD" {
		t.Fatalf("onZero dipanggil salah: %v", zero)
	}

	if m.Count() != 0 {
		t.Fatalf("count = %d, want 0", m.Count())
	}
}

func TestManagerConcurrent(t *testing.T) {
	m := NewManager(func(string) error { return nil }, func(string) error { return nil })

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				_ = m.Subscribe("FOREXCOM:XAUUSD")
				_ = m.Unsubscribe("FOREXCOM:XAUUSD")
			}
		}()
	}
	wg.Wait()

	if m.Count() != 0 {
		t.Fatalf("count = %d, want 0 (ref count tidak balance)", m.Count())
	}
}
