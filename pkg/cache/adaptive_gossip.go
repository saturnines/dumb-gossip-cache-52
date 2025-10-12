package cache

import (
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/vmihailenco/msgpack/v5"
)

type adaptiveGossipHandler struct {
	// This is prolly doing too much but whatever..
	cache      *Cache
	broadcasts *memberlist.TransmitLimitedQueue

	// gossip state
	writeRate  atomic.Int64
	lastWrite  atomic.Int64
	lastGossip atomic.Int64

	decayInterval time.Duration
	gossipFloor   time.Duration
	stopDecay     chan struct{}
	rng           *rand.Rand

	// storing threshholds
	highRate   int64
	mediumRate int64
	lowRate    int64
}

func newAdaptiveGossipHandler(c *Cache, ml *memberlist.Memberlist, retransmitMult int, decayInterval, gossipFloor time.Duration) *adaptiveGossipHandler {
	g := &adaptiveGossipHandler{
		cache: c,
		broadcasts: &memberlist.TransmitLimitedQueue{
			NumNodes:       func() int { return ml.NumMembers() },
			RetransmitMult: retransmitMult,
		},
		decayInterval: decayInterval,
		gossipFloor:   gossipFloor,
		stopDecay:     make(chan struct{}),
		rng:           rand.New(rand.NewSource(time.Now().UnixNano())),

		// thresh rates
		highRate:   c.opts.HighRateThreshold,
		mediumRate: c.opts.MediumRateThreshold,
		lowRate:    c.opts.LowRateThreshold,
	}

	go g.decayLoop()
	return g
}

func (g *adaptiveGossipHandler) stop() {
	close(g.stopDecay)
}

func (g *adaptiveGossipHandler) recordWrite() {
	g.writeRate.Add(100) // Bump rate on write
	g.lastWrite.Store(time.Now().UnixNano())
}

func (g *adaptiveGossipHandler) decayLoop() {
	ticker := time.NewTicker(g.decayInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			current := g.writeRate.Load()
			g.writeRate.Store(current / 2) // Exponential decay
		case <-g.stopDecay:
			return
		}
	}
}

func (g *adaptiveGossipHandler) broadcast(msg GossipMessage) {
	g.recordWrite() // Track write activity

	buf, err := msgpack.Marshal(&msg)
	if err != nil {
		return
	}
	g.broadcasts.QueueBroadcast(&broadcast{msg: buf})
}

// Memberlist Delegate methods
func (g *adaptiveGossipHandler) NodeMeta(limit int) []byte {
	return nil
}

func (g *adaptiveGossipHandler) NotifyMsg(b []byte) {
	var msg GossipMessage
	if err := msgpack.Unmarshal(b, &msg); err != nil {
		return
	}

	// Wake up on incoming gossip
	g.recordWrite()
	g.cache.applyGossip(msg)
}

func (g *adaptiveGossipHandler) GetBroadcasts(overhead, limit int) [][]byte {
	// This entire thing is diabolical, I'm not sure if my math is right
	rate := g.writeRate.Load()
	now := time.Now().UnixNano()
	lastGossip := g.lastGossip.Load()

	// FLOOR: Always gossip at least once per gossipFloor duration
	if now-lastGossip >= g.gossipFloor.Nanoseconds() {
		g.lastGossip.Store(now)
		// Bound the limit to prevent unbounded queue growth
		n := limit / 2
		if n < 1 {
			n = 1
		}
		return g.broadcasts.GetBroadcasts(overhead, n)
	}

	// Adaptive gossip based on write rate
	if rate > g.highRate {
		g.lastGossip.Store(now)
		n := limit / 2
		if n < 1 {
			n = 1
		}
		return g.broadcasts.GetBroadcasts(overhead, n)
	} else if rate > g.mediumRate {
		g.lastGossip.Store(now)
		n := limit / 4
		if n < 1 {
			n = 1
		}
		return g.broadcasts.GetBroadcasts(overhead, n)
	} else if rate > g.lowRate {
		if g.rng.Intn(100) < 40 {
			g.lastGossip.Store(now)
			n := limit / 2
			if n < 1 {
				n = 1
			}
			return g.broadcasts.GetBroadcasts(overhead, n)
		}
	}

	return nil
}

func (g *adaptiveGossipHandler) LocalState(join bool) []byte {
	snap := g.cache.storage.snapshot(false)
	buf, _ := msgpack.Marshal(&snap)
	return buf
}

func (g *adaptiveGossipHandler) MergeRemoteState(buf []byte, join bool) {
	var remote map[string]*Entry
	if err := msgpack.Unmarshal(buf, &remote); err != nil {
		return
	}

	for k, e := range remote {
		g.cache.applyGossip(GossipMessage{
			Key:       k,
			Value:     e.Value,
			Version:   e.Version,
			ExpireAt:  e.ExpireAt,
			Tombstone: e.Tombstone,
			OriginID:  e.OriginID, // Use the stored origin, not local
		})
	}
}
