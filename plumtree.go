package plumtree

import (
	"errors"
	"fmt"
	"log"
	"slices"
	"sync"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
)

type sharedConfig struct {
	self             data.Node
	config           Config
	gossipMsgHandler func(tree TreeMetadata, msgType string, msg []byte, s data.Node) bool
	directMsgHandler func(tree TreeMetadata, msgType string, msg []byte, s data.Node)
	logger           *log.Logger
}

type Plumtree struct {
	shared                 *sharedConfig
	protocol               MembershipProtocol
	peers                  []hyparview.Peer
	trees                  map[string]*Tree
	msgCh                  chan ReceivedPlumtreeMessage
	msgSubscription        transport.Subscription
	msgHandlers            map[MessageType]func(msg []byte, sender hyparview.Peer)
	treeConstructedHandler func(tree TreeMetadata)
	treeDestroyedHandler   func(tree TreeMetadata)
	lock                   *sync.Mutex
}

func NewPlumtree(config Config, protocol MembershipProtocol, logger *log.Logger) *Plumtree {
	p := &Plumtree{
		shared: &sharedConfig{
			config:           config,
			self:             protocol.Self(),
			gossipMsgHandler: func(m TreeMetadata, t string, b []byte, s data.Node) bool { return true },
			directMsgHandler: func(m TreeMetadata, t string, b []byte, s data.Node) {},
			logger:           logger,
		},
		protocol:               protocol,
		peers:                  protocol.GetPeers(config.Fanout),
		trees:                  make(map[string]*Tree),
		msgCh:                  make(chan ReceivedPlumtreeMessage),
		treeConstructedHandler: func(tree TreeMetadata) {},
		treeDestroyedHandler:   func(tree TreeMetadata) {},
		lock:                   new(sync.Mutex),
	}
	p.msgHandlers = map[MessageType]func(msgAny []byte, sender hyparview.Peer){
		GOSSIP_MSG_TYPE: p.onGossip,
		DIRECT_MSG_TYPE: p.onDirect,
		PRUNE_MSG_TYPE:  p.onPrune,
		IHAVE_MSG_TYPE:  p.onIHave,
		GRAFT_MSG_TYPE:  p.onGraft,
	}
	p.msgSubscription = p.processMsgs()
	p.protocol.OnPeerUp(p.onPeerUp)
	p.protocol.OnPeerDown(p.onPeerDown)
	p.protocol.AddCustomMsgHandler(p.handleMsg)
	p.shared.logger.Println(p.shared.self.ID, "-", "Plumtree initialized", "peers", p.peers)
	return p
}

// unlocked
func (p *Plumtree) Join(id, address string) error {
	return p.protocol.Join(id, address)
}

// unlocked
func (p *Plumtree) Leave() {
	// p.msgSubscription.Unsubscribe()
	p.shared.logger.Println("pt leave")
	p.protocol.Leave()
	p.shared.logger.Println("pt left")
	// for _, t := range p.trees {
	// 	t.stopCh <- struct{}{}
	// }
}

// unlocked
func (p *Plumtree) ListenAddress() string {
	return p.shared.self.ListenAddress
}

// locked
func (p *Plumtree) ConstructTree(metadata TreeMetadata) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	tree := NewTree(p.shared, metadata, slices.Clone(p.peers), p.lock)
	p.shared.logger.Println(p.shared.self.ID, "-", "tree created", metadata.Id)
	p.trees[metadata.Id] = tree
	if p.treeConstructedHandler != nil {
		p.lock.Unlock()
		p.treeConstructedHandler(tree.metadata)
		p.lock.Lock()
	}
	return nil
}

// locked
func (p *Plumtree) DestroyTree(metadata TreeMetadata) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if _, ok := p.trees[metadata.Id]; !ok {
		return fmt.Errorf("no tree with id=%s found", metadata.Id)
	}
	p.shared.logger.Println(p.shared.self.ID, "-", "sending destroy gossip msg", metadata)
	p.shared.logger.Println(p.shared.self.ID, "-", "trees", p.trees)
	delete(p.trees, metadata.Id)
	if p.treeDestroyedHandler != nil {
		p.lock.Unlock()
		p.treeDestroyedHandler(metadata)
		p.lock.Lock()
	}
	p.shared.logger.Println(p.shared.self.ID, "-", "trees", p.trees)
	return nil
}

// locked
func (p *Plumtree) Gossip(treeId string, msgType string, msg []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.shared.logger.Println(p.shared.self.ID, "-", "Gossiping message")
	if tree, ok := p.trees[treeId]; !ok {
		return fmt.Errorf("no tree with id=%s found", treeId)
	} else {
		msgId, err := makeMsgID(p.shared.self.ID, msg)
		if err != nil {
			p.shared.logger.Println(p.shared.self.ID, "-", "Error creating hash:", err)
			return err
		}
		payload := PlumtreeCustomMessage{
			Metadata: tree.metadata,
			MsgType:  msgType,
			MsgId:    msgId,
			Msg:      msg,
			Round:    0,
		}
		return tree.Gossip(payload)
	}
}

// locked
func (p *Plumtree) SendToParent(treeId string, msgType string, msg []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.shared.logger.Println(p.shared.self.ID, "-", "send to parent", p.peers)
	if tree, ok := p.trees[treeId]; !ok {
		return fmt.Errorf("no tree with id=%s found", treeId)
	} else {
		err := p.sendDirectMsg(treeId, msgType, msg, tree.parent.Conn)
		if err != nil {
			return fmt.Errorf("error sending %s: %v", msgType, err)
		}
		return nil
	}
}

// locked by caller
// pissibly update in the future if other callers appear
func (p *Plumtree) sendDirectMsg(treeId string, msgType string, msg []byte, receiver transport.Conn) error {
	p.shared.logger.Println(p.shared.self.ID, "-", "Sending message")
	if tree, ok := p.trees[treeId]; !ok {
		return fmt.Errorf("no tree with id=%s found", treeId)
	} else {
		msgId, err := makeMsgID(p.shared.self.ID, msg)
		if err != nil {
			p.shared.logger.Println(p.shared.self.ID, "-", "Error creating hash:", err)
			return err
		}
		payload := PlumtreeCustomMessage{
			Metadata: tree.metadata,
			MsgType:  msgType,
			MsgId:    msgId,
			Msg:      msg,
			Round:    0,
		}
		return tree.SendDirectMsg(payload, receiver)
	}
}

// locked
func (p *Plumtree) HasParent(treeId string) bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.shared.logger.Println(p.shared.self.ID, "-", "Get parent", p.peers)
	tree, ok := p.trees[treeId]
	p.shared.logger.Println(p.shared.self.ID, "-", tree.parent)
	return ok && tree.parent != nil
}

// locked
func (p *Plumtree) GetPeersNum() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.shared.logger.Println(p.shared.self.ID, "-", "Get peers num")
	p.shared.logger.Println(p.shared.self.ID, "-", p.peers)
	return len(p.peers)
}

// locked
func (p *Plumtree) GetChildren(treeId string) ([]data.Node, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.shared.logger.Println(p.shared.self.ID, "-", "Get children", p.peers)
	if tree, ok := p.trees[treeId]; !ok {
		return nil, fmt.Errorf("no tree with id=%s found", treeId)
	} else {
		children := tree.eagerPushPeers
		length := len(children)
		if tree.parent != nil {
			length--
		}
		if length < 0 {
			length = 0
		}
		// todo: lock of unlocked mutex ???????????
		result := make([]data.Node, length)
		j := 0
		for _, c := range children {
			if tree.parent != nil && tree.parent.Node.ID == c.Node.ID {
				continue
			}
			result[j] = c.Node
			j++
		}
		return result, nil
	}
}

// unlocked
func (p *Plumtree) OnGossip(handler func(m TreeMetadata, t string, b []byte, s data.Node) bool) {
	p.shared.gossipMsgHandler = handler
}

// unlocked
func (p *Plumtree) OnDirect(handler func(m TreeMetadata, t string, b []byte, s data.Node)) {
	p.shared.directMsgHandler = handler
}

// unlocked
func (p *Plumtree) OnTreeConstructed(handler func(tree TreeMetadata)) {
	p.treeConstructedHandler = handler
}

// unlocked
func (p *Plumtree) OnTreeDestroyed(handler func(tree TreeMetadata)) {
	p.treeDestroyedHandler = handler
}

// locked
func (p *Plumtree) handleMsg(msg []byte, sender transport.Conn) error {
	p.lock.Lock()
	p.shared.logger.Println(p.shared.self.ID, "-", "Custom message handler invoked")
	p.shared.logger.Println(p.shared.self.ID, "-", "peers", p.peers)
	p.shared.logger.Println(p.shared.self.ID, "-", "sender", sender)
	index := slices.IndexFunc(p.peers, func(peer hyparview.Peer) bool {
		return sender != nil && peer.Conn != nil && peer.Conn.GetAddress() == sender.GetAddress()
	})
	if index < 0 {
		p.lock.Unlock()
		return errors.New("could not find peer in eager or lazy push peers")
	}
	peer := p.peers[index]
	p.lock.Unlock()
	p.shared.logger.Printf("%s - received from %s\n", p.shared.self.ID, peer.Node.ID)
	p.msgCh <- ReceivedPlumtreeMessage{MsgBytes: msg, Sender: peer}
	return nil
}

// locked
func (p *Plumtree) processMsgs() transport.Subscription {
	p.shared.logger.Println(p.shared.self.ID, "-", "Message subscription started")
	return transport.Subscribe(p.msgCh, func(received ReceivedPlumtreeMessage) {
		p.shared.logger.Println(p.shared.self.ID, "-", "Received message in subscription handler", received.MsgBytes)
		p.lock.Lock()
		defer p.lock.Unlock()
		msgType := GetMsgType(received.MsgBytes)
		handler := p.msgHandlers[msgType]
		if handler == nil {
			p.shared.logger.Printf("%s - no handler found for message type %v", p.shared.self.ID, msgType)
			return
		}
		payload, err := transport.GetPayload(received.MsgBytes)
		if err != nil {
			p.shared.logger.Println(p.shared.self.ID, "-", err)
			return
		}
		handler(payload, received.Sender)
	})
}

// locked
func (p *Plumtree) onPeerUp(peer hyparview.Peer) {
	p.shared.logger.Printf("%s - Processing onPeerUp peer: %v\n", p.shared.self.ID, peer.Node.ID)
	p.lock.Lock()
	defer p.lock.Unlock()
	p.shared.logger.Println(p.shared.self.ID, "-", "peers", p.peers)
	if !slices.ContainsFunc(p.peers, func(p hyparview.Peer) bool {
		return p.Node.ID == peer.Node.ID
	}) {
		p.peers = append(p.peers, peer)
		p.shared.logger.Printf("%s - Added peer %v to peers\n", p.shared.self.ID, peer.Node.ID)
		for _, tree := range p.trees {
			tree.onPeerUp(peer)
		}
	}
	p.shared.logger.Println(p.shared.self.ID, "-", "peers", p.peers)
}

// locked
func (p *Plumtree) onPeerDown(peer hyparview.Peer) {
	p.shared.logger.Printf("%s - Processing onPeerDown peer: %v\n", p.shared.self.ID, peer.Node.ID)
	p.lock.Lock()
	defer p.lock.Unlock()
	p.shared.logger.Println(p.shared.self.ID, "-", "peers", p.peers)
	p.peers = slices.DeleteFunc(p.peers, func(p hyparview.Peer) bool {
		return p.Node.ID == peer.Node.ID
	})
	p.shared.logger.Println(p.shared.self.ID, "-", "peers", p.peers)
	for _, tree := range p.trees {
		tree.onPeerDown(peer)
	}
}
