package cache

import (
	"sync"
	"time"
)

type storage struct {
	mu   sync.RWMutex
	data map[string]*Entry
}

func newStorage() *storage {
	return &storage{
		data: make(map[string]*Entry),
	}
}

func (s *storage) set(key string, entry *Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = entry
}

func (s *storage) get(key string) (*Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok || e == nil {
		return nil, false
	}
	// Return a copy to prevent external modification
	copy := *e
	return &copy, true
}

func (s *storage) delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

func (s *storage) compareAndSet(key string, entry *Entry, compareFn func(*Entry) bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.data[key]
	if !exists || compareFn(existing) {
		s.data[key] = entry
		return true
	}
	return false
}

func (s *storage) update(key string, updateFn func(*Entry) *Entry) *Entry {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.data[key]
	newEntry := updateFn(existing)
	if newEntry != nil {
		s.data[key] = newEntry
	}
	return newEntry
}

func (s *storage) snapshot(includeTombstones bool) map[string]*Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := make(map[string]*Entry, len(s.data))
	now := time.Now()

	for k, v := range s.data {
		if !includeTombstones && v.Tombstone {
			continue
		}
		if !v.ExpireAt.IsZero() && now.After(v.ExpireAt) {
			continue
		}
		cp := *v
		snap[k] = &cp
	}
	return snap
}

func (s *storage) sweep(expireFn func(string, *Entry) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, v := range s.data {
		if expireFn(k, v) {
			delete(s.data, k)
		}
	}
}
