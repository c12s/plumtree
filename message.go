package plumtree

import (
	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
)

const (
	GOSSIP_MSG_TYPE data.MessageType = data.UNKNOWN + 1
	DIRECT_MSG_TYPE                  = data.UNKNOWN + 2
	PRUNE_MSG_TYPE                   = data.UNKNOWN + 3
	IHAVE_MSG_TYPE                   = data.UNKNOWN + 4
	FORGET_MSG_TYPE                  = data.UNKNOWN + 5
	GRAFT_MSG_TYPE                   = data.UNKNOWN + 6
)

type PlumtreeCustomMessage struct {
	Metadata TreeMetadata
	MsgType  string
	Msg      []byte
	MsgId    []byte
	Round    int
}

type PlumtreePruneMessage struct {
	Metadata TreeMetadata
}

type PlumtreeIHaveMessage struct {
	Metadata TreeMetadata
	MsgIds   [][]byte
}

type PlumtreeForgetMessage struct {
	Metadata TreeMetadata
	MsgId    []byte
}

type PlumtreeGraftMessage struct {
	Metadata TreeMetadata
	MsgId    []byte
}

type ReceivedPlumtreeMessage struct {
	MsgBytes []byte
	Sender   hyparview.Peer
}
