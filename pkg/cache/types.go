package cache

import (
	"time"
)

// Entry is the local KV state.
type Entry struct {
	Value     []byte
	Version   uint64    // last right wins prim clock
	ExpireAt  time.Time // zero default = no ttl
	Tombstone bool      // true == deleted
	OriginID  string    // Comparison for tiebreaker
}

// GossipMessage is the wire format.
type GossipMessage struct {
	Key       string
	Value     []byte
	Version   uint64
	ExpireAt  time.Time
	Tombstone bool
	// Tie-break when versions collide
	OriginID string // stable node ID  UUID or addr
}

type Options struct {
	OriginID       string
	RetransmitMult int
	SweepInterval  time.Duration

	// Adaptive gossip options
	AdaptiveGossip bool
	DecayInterval  time.Duration
	GossipFloor    time.Duration

	// Explicit rate thresholds
	HighRateThreshold   int64 // Messages/sec for aggressive gossip (default 50)
	MediumRateThreshold int64 // Messages/sec for normal gossip (default 10)
	LowRateThreshold    int64 // Messages/sec for probabilistic gossip (default 1)
}
