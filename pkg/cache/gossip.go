package cache

import (
	"github.com/hashicorp/memberlist"
	"github.com/vmihailenco/msgpack/v5"
)

type gossipHandler struct {
	cache      *Cache
	broadcasts *memberlist.TransmitLimitedQueue
}

func newGossipHandler(c *Cache, ml *memberlist.Memberlist, retransmitMult int) *gossipHandler {
	return &gossipHandler{
		cache: c,
		broadcasts: &memberlist.TransmitLimitedQueue{
			NumNodes:       func() int { return ml.NumMembers() },
			RetransmitMult: retransmitMult,
		},
	}
}

func (g *gossipHandler) broadcast(msg GossipMessage) {
	buf, err := msgpack.Marshal(&msg)
	if err != nil {
		return
	}
	g.broadcasts.QueueBroadcast(&broadcast{msg: buf})
}

// Close implements the gossipDelegate interface
func (g *gossipHandler) Close() {
	// Standard gossip handler has no resources to clean up
}

// Memberlist Delegate methods
func (g *gossipHandler) NodeMeta(limit int) []byte {
	return nil
}

func (g *gossipHandler) NotifyMsg(b []byte) {
	var msg GossipMessage
	if err := msgpack.Unmarshal(b, &msg); err != nil {
		return
	}
	g.cache.applyGossip(msg)
}

func (g *gossipHandler) GetBroadcasts(overhead, limit int) [][]byte {
	return g.broadcasts.GetBroadcasts(overhead, limit)
}

func (g *gossipHandler) LocalState(join bool) []byte {
	snap := g.cache.storage.snapshot(false)
	buf, _ := msgpack.Marshal(&snap)
	return buf
}

func (g *gossipHandler) MergeRemoteState(buf []byte, join bool) {
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
			OriginID:  e.OriginID, // Use e.OriginID, not g.cache.opts.OriginID
		})
	}
}
