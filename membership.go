package plumtree

import (
	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
)

type MembershipProtocol interface {
	Join(id, address string) error
	Leave()
	Self() data.Node
	GetPeers(fanout int) []hyparview.Peer
	OnPeerUp(handler func(peer hyparview.Peer))
	OnPeerDown(handler func(peer hyparview.Peer))
	AddClientMsgHandler(data.MessageType, func(msg []byte, sender hyparview.Peer))
	GetState() any
}
