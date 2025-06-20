package plumtree

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"slices"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
)

func move(peer hyparview.Peer, from, to *[]hyparview.Peer) {
	if !slices.ContainsFunc(*to, func(p hyparview.Peer) bool {
		return peer.Node.ID == p.Node.ID
	}) {
		*to = append(*to, peer)
	}
	*from = slices.DeleteFunc(*from, func(p hyparview.Peer) bool {
		return peer.Node.ID == p.Node.ID
	})
}

func send(payload any, msgType MessageType, to transport.Conn) error {
	pruneMsgSerialized, err := Serialize(Message{Type: msgType, Payload: payload})
	if err != nil {
		return fmt.Errorf("error serializing %v message: %v", msgType, err)
	}
	err = to.Send(data.Message{
		Type:    data.CUSTOM,
		Payload: pruneMsgSerialized,
	})
	if err != nil {
		return fmt.Errorf("error sending %v message: %v", msgType, err)
	}
	return nil
}

func makeMsgID(nodeID string, msg []byte) ([]byte, error) {
	hashFn := fnv.New64()
	ts := time.Now().Unix()
	tsBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(tsBytes, uint64(ts))
	_, err := hashFn.Write(append(append(msg, []byte(nodeID)...), tsBytes...))
	if err != nil {
		return nil, err
	}
	return hashFn.Sum(nil), nil
}
