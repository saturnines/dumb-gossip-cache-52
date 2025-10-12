package cache

import "time"

type janitor struct {
	interval time.Duration
	storage  *storage
	stop     chan struct{} // note may need to rename
}

func newJanitor(storage *storage, interval time.Duration) *janitor {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &janitor{
		interval: interval,
		storage:  storage,
		stop:     make(chan struct{}),
	}
}

func (j *janitor) start() {
	go j.run()
}

func (j *janitor) stop() {
	close(j.stop)
}

func (j *janitor) run() {
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			j.sweep()
		case <-j.stop:
			return
		}
	}
}

func (j *janitor) sweep() {
	now := time.Now()
	j.storage.sweep(func(key string, entry *Entry) bool {
		// Remove tombstones after one sweep cycle
		if entry.Tombstone {
			return true
		}
		// Remove expired entries
		if !entry.ExpireAt.IsZero() && now.After(entry.ExpireAt) {
			return true
		}
		return false
	})
}
