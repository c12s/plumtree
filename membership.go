package plumtree

import (
	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
)

type MembershipProtocol interface {
	Join(id, address string) error
	Leave()
	Self() data.Node
	GetPeers(fanout int) []hyparview.Peer
	OnPeerUp(handler func(peer hyparview.Peer))
	OnPeerDown(handler func(peer hyparview.Peer))
	AddCustomMsgHandler(func(msg []byte, sender transport.Conn) error)
}
