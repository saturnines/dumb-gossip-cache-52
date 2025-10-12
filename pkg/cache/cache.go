package cache

import (
	"github.com/hashicorp/memberlist"
	"time"
)

type Cache struct {
	ml      *memberlist.Memberlist
	opts    Options
	storage *storage
	gossip  gossipDelegate // ← Use interface
	janitor *janitor
}

type gossipDelegate interface {
	broadcast(GossipMessage)
	NodeMeta(int) []byte
	NotifyMsg([]byte)
	GetBroadcasts(int, int) [][]byte
	LocalState(bool) []byte
	MergeRemoteState([]byte, bool)
}

func New(ml *memberlist.Memberlist, opts Options) *Cache {
	if opts.RetransmitMult <= 0 {
		opts.RetransmitMult = 3
	}
	if opts.SweepInterval <= 0 {
		opts.SweepInterval = 5 * time.Second
	}
	// defaults for adaptive gossip if enabled
	if opts.AdaptiveGossip {
		if opts.DecayInterval <= 0 {
			opts.DecayInterval = 1 * time.Second
		}
		if opts.GossipFloor <= 0 {
			opts.GossipFloor = 30 * time.Second
		}
		// FIX #5: Set threshold defaults
		if opts.HighRateThreshold <= 0 {
			opts.HighRateThreshold = 50
		}
		if opts.MediumRateThreshold <= 0 {
			opts.MediumRateThreshold = 10
		}
		if opts.LowRateThreshold <= 0 {
			opts.LowRateThreshold = 1
		}
	}

	c := &Cache{
		ml:      ml,
		opts:    opts,
		storage: newStorage(),
	}

	// NEW: Choose gossip handler based on options
	if opts.AdaptiveGossip {
		c.gossip = newAdaptiveGossipHandler(c, ml, opts.RetransmitMult, opts.DecayInterval, opts.GossipFloor)
	} else {
		c.gossip = newGossipHandler(c, ml, opts.RetransmitMult)
	}

	c.janitor = newJanitor(c.storage, opts.SweepInterval)
	c.janitor.start()

	return c
}

func (c *Cache) Close() {
	c.janitor.stop()

	if ag, ok := c.gossip.(*adaptiveGossipHandler); ok {
		ag.stop()
	}
}

// Public API
func (c *Cache) Set(key string, value []byte, ttl time.Duration) {
	now := time.Now()
	exp := time.Time{}
	if ttl > 0 {
		exp = now.Add(ttl)
	}
	baseVer := uint64(now.UnixNano())

	entry := c.storage.update(key, func(existing *Entry) *Entry {
		ver := baseVer
		if existing != nil && existing.Version >= ver {
			ver = existing.Version + 1
		}

		return &Entry{
			Value:     value,
			Version:   ver,
			ExpireAt:  exp,
			Tombstone: false,
			OriginID:  c.opts.OriginID,
		}
	})

	c.gossip.broadcast(GossipMessage{
		Key:       key,
		Value:     value,
		Version:   entry.Version,
		ExpireAt:  exp,
		Tombstone: false,
		OriginID:  c.opts.OriginID,
	})
}

func (c *Cache) Get(key string) ([]byte, bool) {
	entry, ok := c.storage.get(key)
	if !ok || entry.Tombstone {
		return nil, false
	}
	if isExpired(entry) {
		return nil, false
	}
	return append([]byte(nil), entry.Value...), true
}

func (c *Cache) Delete(key string) {
	now := time.Now()
	baseVer := uint64(now.UnixNano())

	entry := c.storage.update(key, func(existing *Entry) *Entry {
		ver := baseVer
		if existing != nil && existing.Version >= ver {
			ver = existing.Version + 1
		}

		return &Entry{
			Version:   ver,
			Tombstone: true,
			OriginID:  c.opts.OriginID,
		}
	})

	c.gossip.broadcast(GossipMessage{
		Key:       key,
		Version:   entry.Version,
		Tombstone: true,
		OriginID:  c.opts.OriginID,
	})
}

// Internal gossip application
func (c *Cache) applyGossip(msg GossipMessage) {
	newEntry := &Entry{
		Value:     append([]byte(nil), msg.Value...),
		Version:   msg.Version,
		ExpireAt:  msg.ExpireAt,
		Tombstone: msg.Tombstone,
		OriginID:  msg.OriginID,
	}

	c.storage.compareAndSet(msg.Key, newEntry, func(existing *Entry) bool {
		if existing == nil {
			return true
		}
		return isNewer(msg, existing)
	})
}

// forward to gossip handler
func (c *Cache) NodeMeta(limit int) []byte {
	return c.gossip.NodeMeta(limit)
}

func (c *Cache) NotifyMsg(b []byte) {
	c.gossip.NotifyMsg(b)
}

func (c *Cache) GetBroadcasts(overhead, limit int) [][]byte {
	return c.gossip.GetBroadcasts(overhead, limit)
}

func (c *Cache) LocalState(join bool) []byte {
	return c.gossip.LocalState(join)
}

func (c *Cache) MergeRemoteState(buf []byte, join bool) {
	c.gossip.MergeRemoteState(buf, join)
}

// Helper functions
func isNewer(msg GossipMessage, cur *Entry) bool {
	if msg.Version != cur.Version {
		return msg.Version > cur.Version
	}
	// lexicographically lowest OriginID wins
	if msg.OriginID == "" || cur.OriginID == "" {
		return false
	}
	return msg.OriginID < cur.OriginID
}

func isExpired(e *Entry) bool {
	return !e.ExpireAt.IsZero() && time.Now().After(e.ExpireAt)
}
