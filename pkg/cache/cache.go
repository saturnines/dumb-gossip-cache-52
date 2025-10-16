package cache

import (
	"github.com/hashicorp/memberlist"
	"time"
)

type Cache struct {
	ml      *memberlist.Memberlist
	mlOwned bool

	opts    Options
	storage *storage
	gossip  gossipDelegate
	janitor *janitor
}

type gossipDelegate interface {
	broadcast(GossipMessage)
	NodeMeta(int) []byte
	NotifyMsg([]byte)
	GetBroadcasts(int, int) [][]byte
	LocalState(bool) []byte
	MergeRemoteState([]byte, bool)
	Close()
}

// dummy placeholders should be safe
type nullGossip struct{}

func (nullGossip) broadcast(GossipMessage)         {}
func (nullGossip) NodeMeta(int) []byte             { return nil }
func (nullGossip) NotifyMsg([]byte)                {}
func (nullGossip) GetBroadcasts(int, int) [][]byte { return nil }
func (nullGossip) LocalState(bool) []byte          { return nil }
func (nullGossip) MergeRemoteState([]byte, bool)   {}
func (nullGossip) Close()                          {}

func NewWithConfig(cfg *memberlist.Config, opts Options) (*Cache, error) {
	setOptionDefaults(&opts)

	c := &Cache{
		opts:    opts,
		storage: newStorage(),
		gossip:  nullGossip{},
		mlOwned: true,
	}

	// checks if OriginID is stable.
	if c.opts.OriginID == "" && cfg != nil && cfg.Name != "" {
		c.opts.OriginID = cfg.Name
	}

	cfg.Delegate = c

	ml, err := memberlist.Create(cfg)
	if err != nil {
		return nil, err
	}
	c.ml = ml

	// actual handler after ml is ready.
	if opts.AdaptiveGossip {
		c.gossip = newAdaptiveGossipHandler(c, ml, opts.RetransmitMult, opts.DecayInterval, opts.GossipFloor)
	} else {
		c.gossip = newGossipHandler(c, ml, opts.RetransmitMult)
	}

	c.janitor = newJanitor(c.storage, opts.SweepInterval)
	c.janitor.start()

	return c, nil
}

// New uses an existing memberlist. Caller owns ml.
func New(ml *memberlist.Memberlist, opts Options) *Cache {
	setOptionDefaults(&opts)

	c := &Cache{
		ml:      ml,
		opts:    opts,
		storage: newStorage(),
		gossip:  nullGossip{}, // swap below
	}

	if c.opts.OriginID == "" && ml != nil && ml.LocalNode() != nil {
		c.opts.OriginID = ml.LocalNode().Name
	}

	if opts.AdaptiveGossip {
		c.gossip = newAdaptiveGossipHandler(c, ml, opts.RetransmitMult, opts.DecayInterval, opts.GossipFloor)
	} else {
		c.gossip = newGossipHandler(c, ml, opts.RetransmitMult)
	}

	c.janitor = newJanitor(c.storage, opts.SweepInterval)
	c.janitor.start()

	return c
}

// Memberlist uses the underlying memberlist
func (c *Cache) Memberlist() *memberlist.Memberlist {
	return c.ml
}

func (c *Cache) Close() {
	if c.janitor != nil {
		c.janitor.stop()
	}
	if c.gossip != nil {
		c.gossip.Close()
	}
	if c.mlOwned && c.ml != nil {
		_ = c.ml.Shutdown()
	}
}

//public API

func (c *Cache) Set(key string, value []byte, ttl time.Duration) {
	now := time.Now()
	exp := time.Time{}
	if ttl > 0 {
		exp = now.Add(ttl)
	}

	entry := c.storage.update(key, func(existing *Entry) *Entry {
		ver := bumpVersion(getVersion(existing))
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
	if !ok || entry.Tombstone || isExpired(entry) {
		return nil, false
	}
	return append([]byte(nil), entry.Value...), true
}

func (c *Cache) Delete(key string) {
	entry := c.storage.update(key, func(existing *Entry) *Entry {
		ver := bumpVersion(getVersion(existing))
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

// forward call to c.gossip i think

func (c *Cache) NodeMeta(limit int) []byte { return c.gossip.NodeMeta(limit) }
func (c *Cache) NotifyMsg(b []byte)        { c.gossip.NotifyMsg(b) }
func (c *Cache) GetBroadcasts(overhead, limit int) [][]byte {
	return c.gossip.GetBroadcasts(overhead, limit)
}
func (c *Cache) LocalState(join bool) []byte            { return c.gossip.LocalState(join) }
func (c *Cache) MergeRemoteState(buf []byte, join bool) { c.gossip.MergeRemoteState(buf, join) }

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

// helpers for adaptive

func setOptionDefaults(opts *Options) {
	if opts.RetransmitMult <= 0 {
		opts.RetransmitMult = 3
	}
	if opts.SweepInterval <= 0 {
		opts.SweepInterval = 5 * time.Second
	}
	if opts.AdaptiveGossip {
		if opts.DecayInterval <= 0 {
			opts.DecayInterval = 1 * time.Second
		}
		if opts.GossipFloor <= 0 {
			opts.GossipFloor = 30 * time.Second
		}
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
}

func bumpVersion(prev uint64) uint64 {
	v := uint64(time.Now().UnixNano())
	if prev >= v {
		v = prev + 1
	}
	return v
}

func getVersion(e *Entry) uint64 {
	if e == nil {
		return 0
	}
	return e.Version
}

func isNewer(msg GossipMessage, cur *Entry) bool {
	if msg.Version != cur.Version {
		return msg.Version > cur.Version
	}
	if msg.OriginID == "" || cur.OriginID == "" {
		return false
	}
	return msg.OriginID < cur.OriginID // lexicographically lowest wins
}

func isExpired(e *Entry) bool {
	return !e.ExpireAt.IsZero() && time.Now().After(e.ExpireAt)
}
