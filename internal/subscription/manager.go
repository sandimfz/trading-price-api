package subscription

import "sync"

// Manager adalah symbol subscription dengan ref counting.
// Aman dipanggil dari banyak goroutine (HTTP handler, WS handler).
type Manager struct {
	mu      sync.Mutex
	refs    map[string]int
	onFirst func(symbol string) error
	onZero  func(symbol string) error
}

// NewManager membuat manager; onFirst dipanggil saat ref count 0->1,
// onZero saat ref count turun ke 0.
func NewManager(onFirst, onZero func(symbol string) error) *Manager {
	return &Manager{
		refs:    make(map[string]int),
		onFirst: onFirst,
		onZero:  onZero,
	}
}

// Subscribe menambah ref count simbol, memanggil onFirst untuk subscribe pertama.
func (m *Manager) Subscribe(symbol string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.refs[symbol]++
	if m.refs[symbol] == 1 {
		return m.onFirst(symbol)
	}
	return nil
}

// Unsubscribe mengurangi ref count, memanggil onZero saat mencapai 0.
func (m *Manager) Unsubscribe(symbol string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.refs[symbol]; !ok {
		return nil
	}
	m.refs[symbol]--
	if m.refs[symbol] <= 0 {
		delete(m.refs, symbol)
		return m.onZero(symbol)
	}
	return nil
}

// ActiveSymbols mengembalikan salinan simbol yang sedang di-ref.
func (m *Manager) ActiveSymbols() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	symbols := make([]string, 0, len(m.refs))
	for s := range m.refs {
		symbols = append(symbols, s)
	}
	return symbols
}

// Count mengembalikan jumlah simbol yang sedang di-ref.
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.refs)
}
