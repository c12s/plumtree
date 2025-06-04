package plumtree

import (
	"github.com/c12s/hyparview/hyparview"
)

type MessageType int8

const (
	GOSSIP_MSG_TYPE MessageType = iota
	DIRECT_MSG_TYPE
	PRUNE_MSG_TYPE
	IHAVE_MSG_TYPE
	GRAFT_MSG_TYPE
	UNKNOWN_MSG_TYPE
)

func KnownMsgTypes() []MessageType {
	return []MessageType{GOSSIP_MSG_TYPE, DIRECT_MSG_TYPE, PRUNE_MSG_TYPE, IHAVE_MSG_TYPE, GRAFT_MSG_TYPE}
}

type Message struct {
	Type    MessageType
	Payload any
}

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

type PlumtreeGraftMessage struct {
	Metadata TreeMetadata
	MsgId    []byte
}

type ReceivedPlumtreeMessage struct {
	MsgBytes []byte
	Sender   hyparview.Peer
}
