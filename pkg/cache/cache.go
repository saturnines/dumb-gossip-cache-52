package cache

import (
	"github.com/hashicorp/memberlist"
	"time"
)

type Cache struct {
	ml      *memberlist.Memberlist
	opts    Options
	storage *storage
	gossip  *gossipHandler
	janitor *janitor
}

func New(ml *memberlist.Memberlist, opts Options) *Cache {
	if opts.RetransmitMult <= 0 {
		opts.RetransmitMult = 3
	}
	if opts.SweepInterval <= 0 {
		opts.SweepInterval = 5 * time.Second
	}

	c := &Cache{
		ml:      ml,
		opts:    opts,
		storage: newStorage(),
	}

	c.gossip = newGossipHandler(c, ml, opts.RetransmitMult)
	c.janitor = newJanitor(c.storage, opts.SweepInterval)
	c.janitor.start()

	return c
}

func (c *Cache) Close() {
	c.janitor.stop()
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
	}

	c.storage.compareAndSet(msg.Key, newEntry, func(existing *Entry) bool {
		if existing == nil {
			return true
		}
		return isNewer(msg, existing, c.opts.OriginID)
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
