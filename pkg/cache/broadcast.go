package cache

import "github.com/hashicorp/memberlist"

type broadcast struct {
	msg []byte
}

func (b *broadcast) Invalidates(memberlist.Broadcast) bool {
	return false
}

func (b *broadcast) Message() []byte {
	return b.msg
}

func (b *broadcast) Finished() {}
