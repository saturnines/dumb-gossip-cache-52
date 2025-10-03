package cache

import (
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/vmihailenco/msgpack/v5"
)

type Cache struct {
	ml          *memberlist.Memberlist
	opts        Options
	mu          sync.RWMutex
	data        map[string]*Entry
	broadcasts  *memberlist.TransmitLimitedQueue
	stopJanitor chan struct{}
}

// New wires a cache onto an existing memberlist.
// set c as Delegate on the memberlist, e.g. cfg.Delegate = c (in caller) or this won't work (note to self)
func New(ml *memberlist.Memberlist, opts Options) *Cache {
	if opts.RetransmitMult <= 0 {
		opts.RetransmitMult = 3
	}
	if opts.SweepInterval <= 0 {
		opts.SweepInterval = 5 * time.Second
	}
	c := &Cache{
		ml:          ml,
		opts:        opts,
		data:        make(map[string]*Entry),
		stopJanitor: make(chan struct{}),
	}
	c.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes:       func() int { return ml.NumMembers() },
		RetransmitMult: opts.RetransmitMult,
	}
	go c.janitor()
	return c
}

func (c *Cache) Close() {
	close(c.stopJanitor)
}

// Public Package API
func (c *Cache) Set(key string, value []byte, ttl time.Duration) {
	now := time.Now()
	exp := time.Time{}
	if ttl > 0 {
		exp = now.Add(ttl)
	}
	ver := uint64(now.UnixNano())

	c.mu.Lock()
	c.data[key] = &Entry{Value: value, Version: ver, ExpireAt: exp, Tombstone: false}
	c.mu.Unlock()

	c.gossip(GossipMessage{
		Key: key, Value: value, Version: ver, ExpireAt: exp, Tombstone: false, OriginID: c.opts.OriginID,
	})
}

func (c *Cache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	e, ok := c.data[key]
	c.mu.RUnlock()
	if !ok || e.Tombstone {
		return nil, false
	}
	if expired(e) {
		return nil, false
	}
	return append([]byte(nil), e.Value...), true
}

// Delete is LWW, publish tombstone so nodes know
func (c *Cache) Delete(key string) {
	now := time.Now()
	ver := uint64(now.UnixNano())

	c.mu.Lock()
	if cur, ok := c.data[key]; ok && cur.Version > ver {
		// don't go backwards!
		ver = cur.Version + 1
	}
	c.data[key] = &Entry{Version: ver, Tombstone: true}
	c.mu.Unlock()

	c.gossip(GossipMessage{
		Key: key, Version: ver, Tombstone: true, OriginID: c.opts.OriginID,
	})
}

// memberlist delegate
func (c *Cache) NodeMeta(limit int) []byte { return nil }

func (c *Cache) NotifyMsg(b []byte) {
	var msg GossipMessage
	if err := msgpack.Unmarshal(b, &msg); err != nil {
		return
	}
	c.apply(msg)
}

func (c *Cache) GetBroadcasts(overhead, limit int) [][]byte {
	return c.broadcasts.GetBroadcasts(overhead, limit)
}

// LocalStatejust returns a snap shot of a node's cache
// Memberlist should useit when a node joins or resyncs so peers can get a full copy
func (c *Cache) LocalState(join bool) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Copy live state only, skip expired to keep payload small
	snap := make(map[string]*Entry, len(c.data))
	now := time.Now()
	for k, v := range c.data {
		if v.Tombstone || (!v.ExpireAt.IsZero() && now.After(v.ExpireAt)) {
			continue
		}
		cp := *v
		snap[k] = &cp
	}
	buf, _ := msgpack.Marshal(&snap)
	return buf
}

// Func to catch up if gossip messages are missed
func (c *Cache) MergeRemoteState(buf []byte, join bool) {
	var remote map[string]*Entry
	if err := msgpack.Unmarshal(buf, &remote); err != nil {
		return
	}
	// Convert into LWW messages to reuse apply() logic.
	nowOrigin := c.opts.OriginID // only for tie break symmetry
	for k, e := range remote {
		c.apply(GossipMessage{
			Key: k, Value: e.Value, Version: e.Version, ExpireAt: e.ExpireAt, Tombstone: e.Tombstone, OriginID: nowOrigin,
		})
	}
}

// Internals
func (c *Cache) gossip(m GossipMessage) {
	buf, err := msgpack.Marshal(&m)
	if err != nil {
		return
	}
	c.broadcasts.QueueBroadcast(&broadcast{msg: buf})
}

func (c *Cache) apply(m GossipMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()

	existing, ok := c.data[m.Key]
	if !ok || newer(m, existing, c.opts.OriginID) {
		c.data[m.Key] = &Entry{
			Value:     append([]byte(nil), m.Value...),
			Version:   m.Version,
			ExpireAt:  m.ExpireAt,
			Tombstone: m.Tombstone,
		}
	}
}

func newer(m GossipMessage, cur *Entry, localOrigin string) bool {
	// Primary: higher Version wins.
	if m.Version != cur.Version {
		return m.Version > cur.Version
	}
	// Tiebreaker lexicographically lowest OriginID should win
	if m.OriginID == "" || localOrigin == "" {
		return false
	}
	return m.OriginID < localOrigin
}

func expired(e *Entry) bool {
	return !e.ExpireAt.IsZero() && time.Now().After(e.ExpireAt)
}

type broadcast struct{ msg []byte }

func (b *broadcast) Invalidates(memberlist.Broadcast) bool { return false }
func (b *broadcast) Message() []byte                       { return b.msg }
func (b *broadcast) Finished()                             {}

func (c *Cache) janitor() {
	t := time.NewTicker(c.opts.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			now := time.Now()
			c.mu.Lock()
			for k, v := range c.data {
				if v.Tombstone {
					// Drop tombstones after a grace period, e.g one janitor cycle here
					delete(c.data, k)
					continue
				}
				if !v.ExpireAt.IsZero() && now.After(v.ExpireAt) {
					delete(c.data, k)
				}
			}
			c.mu.Unlock()
		case <-c.stopJanitor:
			return
		}
	}
}
