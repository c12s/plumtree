package plumtree

import (
	"encoding/json"

	"github.com/c12s/hyparview/hyparview"
)

type PlumtreeMessageType int8

const (
	GOSSIP_MSG_TYPE PlumtreeMessageType = iota
	PRUNE_MSG_TYPE
	IHAVE_MSG_TYPE
	GRAFT_MSG_TYPE
)

type PlumtreeGossipMessage struct {
	Metadata TreeMetadata
	MsgType  string
	Msg      []byte
	MsgId    []byte
	Round    int
}

func (m PlumtreeGossipMessage) Serialize() ([]byte, error) {
	payloadSerialized, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	payloadSerialized = append([]byte{byte(GOSSIP_MSG_TYPE)}, payloadSerialized...)
	return payloadSerialized, nil
}

type PlumtreePruneMessage struct {
	Metadata TreeMetadata
}

func (m PlumtreePruneMessage) Serialize() ([]byte, error) {
	payloadSerialized, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	payloadSerialized = append([]byte{byte(PRUNE_MSG_TYPE)}, payloadSerialized...)
	return payloadSerialized, nil
}

type PlumtreeIHaveMessage struct {
	Metadata TreeMetadata
	MsgIds   [][]byte
}

func (m PlumtreeIHaveMessage) Serialize() ([]byte, error) {
	payloadSerialized, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	payloadSerialized = append([]byte{byte(IHAVE_MSG_TYPE)}, payloadSerialized...)
	return payloadSerialized, nil
}

type PlumtreeGraftMessage struct {
	Metadata TreeMetadata
	MsgId    []byte
}

func (m PlumtreeGraftMessage) Serialize() ([]byte, error) {
	payloadSerialized, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	payloadSerialized = append([]byte{byte(GRAFT_MSG_TYPE)}, payloadSerialized...)
	return payloadSerialized, nil
}

type ReceivedPlumtreeMessage struct {
	Metadata      TreeMetadata
	MsgSerialized []byte
	Sender        hyparview.Peer
}
