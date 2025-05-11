package plumtree

import "github.com/c12s/hyparview/data"

type PlumtreeMessageType int8

const (
	GOSSIP_MSG_TYPE PlumtreeMessageType = iota
	PRUNE_MSG_TYPE
	IHAVE_MSG_TYPE
)

type PlumtreeGossipMessage struct {
	Msg   []byte
	MsgId []byte
	Round int
}

type PlumtreeIHaveMessage struct {
	MsgIds [][]byte
}

type ReceivedPlumtreeMessage struct {
	MsgSerialized []byte
	Sender        data.Node
}
