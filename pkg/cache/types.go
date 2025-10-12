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
	//tie breaking.
	OriginID string

	// Retransmit multiplier.
	RetransmitMult int

	// Janitor runs every SweepInterval to drop expired entries.
	SweepInterval time.Duration
}
