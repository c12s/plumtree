package plumtree

import (
	"fmt"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
)

type sharedConfig struct {
	self             data.Node
	config           Config
	gossipMsgHandler func(tree TreeMetadata, msgType string, msg []byte, s hyparview.Peer) bool
	directMsgHandler func(tree TreeMetadata, msgType string, msg []byte, s data.Node)
	logger           *log.Logger
}

type Plumtree struct {
	shared   *sharedConfig
	Protocol MembershipProtocol
	peers    []hyparview.Peer
	trees    map[string]*Tree
	msgCh    chan ReceivedPlumtreeMessage
	// msgSubscription transport.Subscription
	// msgHandlers            map[MessageType]func(msg []byte, sender hyparview.Peer)
	treeConstructedHandler func(tree TreeMetadata)
	treeDestroyedHandler   func(tree TreeMetadata)
	peerUpHandler          func(peer hyparview.Peer)
	peerDownHandler        func(peer hyparview.Peer)
	lock                   *sync.Mutex
}

func NewPlumtree(config Config, protocol MembershipProtocol, logger *log.Logger) *Plumtree {
	p := &Plumtree{
		shared: &sharedConfig{
			config:           config,
			self:             protocol.Self(),
			gossipMsgHandler: func(m TreeMetadata, t string, b []byte, s hyparview.Peer) bool { return true },
			directMsgHandler: func(m TreeMetadata, t string, b []byte, s data.Node) {},
			logger:           logger,
		},
		Protocol:               protocol,
		peers:                  protocol.GetPeers(config.Fanout),
		trees:                  make(map[string]*Tree),
		msgCh:                  make(chan ReceivedPlumtreeMessage),
		treeConstructedHandler: func(tree TreeMetadata) {},
		treeDestroyedHandler:   func(tree TreeMetadata) {},
		peerUpHandler:          func(peer hyparview.Peer) {},
		peerDownHandler:        func(peer hyparview.Peer) {},
		lock:                   new(sync.Mutex),
	}
	protocol.AddClientMsgHandler(GOSSIP_MSG_TYPE, func(msg []byte, sender hyparview.Peer) {
		p.lock.Lock()
		defer p.lock.Unlock()
		p.onGossip(msg, sender)
	})
	protocol.AddClientMsgHandler(DIRECT_MSG_TYPE, func(msg []byte, sender hyparview.Peer) {
		p.lock.Lock()
		defer p.lock.Unlock()
		p.onDirect(msg, sender)
	})
	protocol.AddClientMsgHandler(PRUNE_MSG_TYPE, func(msg []byte, sender hyparview.Peer) {
		p.lock.Lock()
		defer p.lock.Unlock()
		p.onPrune(msg, sender)
	})
	protocol.AddClientMsgHandler(IHAVE_MSG_TYPE, func(msg []byte, sender hyparview.Peer) {
		p.lock.Lock()
		defer p.lock.Unlock()
		p.onIHave(msg, sender)
	})
	protocol.AddClientMsgHandler(FORGET_MSG_TYPE, func(msg []byte, sender hyparview.Peer) {
		p.lock.Lock()
		defer p.lock.Unlock()
		p.onForget(msg, sender)
	})
	protocol.AddClientMsgHandler(GRAFT_MSG_TYPE, func(msg []byte, sender hyparview.Peer) {
		p.lock.Lock()
		defer p.lock.Unlock()
		p.onGraft(msg, sender)
	})
	// p.msgSubscription = p.processMsgs()
	p.Protocol.OnPeerUp(p.onPeerUp)
	p.Protocol.OnPeerDown(p.onPeerDown)
	p.shared.logger.Println(p.shared.self.ID, "-", "Plumtree initialized", "peers", p.peers)
	go p.cleanUp()
	return p
}

func (p *Plumtree) cleanUp() {
	for range time.NewTicker(1 * time.Second).C {
		// p.shared.logger.Println("clean up trees")
		removeIds := []string{}
		p.lock.Lock()
		for id, tree := range p.trees {
			// p.shared.logger.Println(id)
			// p.shared.logger.Println(len(tree.receivedMsgs))
			// p.shared.logger.Println(tree.lastMsg)
			if len(tree.receivedMsgs) > 0 && tree.lastMsg+30 < time.Now().Unix() && tree.metadata.NodeID() != p.Protocol.Self().ID {
				removeIds = append(removeIds, id)
			}
		}
		// p.shared.logger.Println(removeIds)
		for _, id := range removeIds {
			delete(p.trees, id)
		}
		p.lock.Unlock()
	}
}

// unlocked
func (p *Plumtree) Join(id, address string) error {
	return p.Protocol.Join(id, address)
}

// unlocked
func (p *Plumtree) Leave() {
	p.shared.logger.Println("pt leave")
	p.Protocol.Leave()
	p.peers = make([]hyparview.Peer, 0)
	p.trees = make(map[string]*Tree)
	p.shared.logger.Println("pt left")
}

// unlocked
func (p *Plumtree) ListenAddress() string {
	return p.shared.self.ListenAddress
}

// locked
func (p *Plumtree) ConstructTree(metadata TreeMetadata) error {
	// p.shared.logger.Println("try lock")
	p.lock.Lock()
	defer p.lock.Unlock()
	tree, ok := p.trees[metadata.Id]
	if !ok {
		tree = NewTree(p.shared, metadata, slices.Clone(p.peers), p.lock)
	} else {
		tree.destroyed = false
	}
	// tree := NewTree(p.shared, metadata, slices.Clone(p.peers), p.lock)
	p.shared.logger.Println(p.shared.self.ID, "-", "tree created", metadata.Id)
	p.trees[metadata.Id] = tree
	if p.treeConstructedHandler != nil {
		// p.lock.Unlock()
		go p.treeConstructedHandler(tree.metadata)
		// // p.shared.logger.Println("try lock")
		// p.lock.Lock()
	}
	return nil
}

// locked
func (p *Plumtree) DestroyTree(metadata TreeMetadata) error {
	p.shared.logger.Println("destroying tree", metadata)
	// p.shared.logger.Println("try lock")
	p.lock.Lock()
	defer p.lock.Unlock()
	if tree, ok := p.trees[metadata.Id]; !ok {
		return fmt.Errorf("no tree with id=%s found", metadata.Id)
	} else {
		p.shared.logger.Println(p.shared.self.ID, "-", "trees", p.trees)
		if p.treeDestroyedHandler != nil {
			p.lock.Unlock()
			p.treeDestroyedHandler(metadata)
			// p.shared.logger.Println("try lock")
			p.lock.Lock()
		}
		tree.destroyed = true
		p.shared.logger.Println(p.shared.self.ID, "-", "trees", p.trees)
		return nil
	}
}

// locked
func (p *Plumtree) Broadcast(treeId string, msgType string, msg []byte) error {
	// p.shared.logger.Println("try lock")
	p.lock.Lock()
	defer p.lock.Unlock()
	p.shared.logger.Println(p.shared.self.ID, "-", "Gossiping message")
	if tree, ok := p.trees[treeId]; !ok || tree == nil || tree.destroyed {
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
		return tree.Broadcast(payload)
	}
}

// locked
func (p *Plumtree) SendDirectMsg(treeId string, msgType string, msg []byte, to transport.Conn) error {
	// p.shared.logger.Println("try lock")
	p.lock.Lock()
	defer p.lock.Unlock()
	p.shared.logger.Println(p.shared.self.ID, "-", "send to parent", p.peers)
	if tree, ok := p.trees[treeId]; !ok || tree == nil || tree.destroyed {
		return fmt.Errorf("no tree with id=%s found", treeId)
	} else {
		p.shared.logger.Println(p.shared.self.ID, "-", "send to parent id=", tree.parent.Node.ID)
		err := p.sendDirectMsg(treeId, msgType, msg, to)
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
	if tree, ok := p.trees[treeId]; !ok || tree == nil || tree.destroyed {
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
	// p.shared.logger.Println("try lock")
	p.lock.Lock()
	defer p.lock.Unlock()
	p.shared.logger.Println(p.shared.self.ID, "-", "Get parent", p.peers)
	tree, ok := p.trees[treeId]
	// p.shared.logger.Println(p.shared.self.ID, "-", tree.parent)
	return ok && tree.parent != nil
}

// locked
func (p *Plumtree) GetPeersNum() int {
	// p.shared.logger.Println("try lock")
	p.lock.Lock()
	defer p.lock.Unlock()
	// p.shared.logger.Println(p.shared.self.ID, "-", "Get peers num")
	// p.shared.logger.Println(p.shared.self.ID, "-", p.peers)
	return len(p.peers)
}

// locked
func (p *Plumtree) GetPeers() []hyparview.Peer {
	// p.shared.logger.Println("try lock")
	p.lock.Lock()
	defer p.lock.Unlock()
	// p.shared.logger.Println(p.shared.self.ID, "-", "Get peers")
	// p.shared.logger.Println(p.shared.self.ID, "-", p.peers)
	return slices.Clone(p.peers)
}

// locked
func (p *Plumtree) GetChildren(treeId string) ([]data.Node, error) {
	// p.shared.logger.Println("try lock")
	p.lock.Lock()
	defer p.lock.Unlock()
	// p.shared.logger.Println(p.shared.self.ID, "-", "Get children", p.peers)
	if tree, ok := p.trees[treeId]; !ok || tree == nil || tree.destroyed {
		return []data.Node{}, fmt.Errorf("no tree with id=%s found", treeId)
	} else {
		children := slices.Clone(tree.eagerPushPeers)
		result := make([]data.Node, 0)
		for _, c := range children {
			result = append(result, c.Node)
		}
		result = slices.DeleteFunc(result, func(n data.Node) bool {
			return tree.parent != nil && tree.parent.Node.ID == n.ID
		})
		return result, nil
	}
}

// unlocked
func (p *Plumtree) OnGossip(handler func(m TreeMetadata, t string, b []byte, s hyparview.Peer) bool) {
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

// unlocked
func (p *Plumtree) OnPeerUp(handler func(hyparview.Peer)) {
	p.peerUpHandler = handler
}

// unlocked
func (p *Plumtree) OnPeerDown(handler func(hyparview.Peer)) {
	p.peerDownHandler = handler
}

// // locked
// func (p *Plumtree) handleMsg(msg []byte, sender transport.Conn) error {
// 	// p.shared.logger.Println("try lock")
// 	p.lock.Lock()
// 	p.shared.logger.Println(p.shared.self.ID, "-", "Custom message handler invoked")
// 	// p.shared.logger.Println(p.shared.self.ID, "-", "peers", p.peers)
// 	p.shared.logger.Println(p.shared.self.ID, "-", "sender", sender)
// 	index := slices.IndexFunc(p.peers, func(peer hyparview.Peer) bool {
// 		return sender != nil && peer.Conn != nil && peer.Conn.GetAddress() == sender.GetAddress()
// 	})
// 	if index < 0 {
// 		p.lock.Unlock()
// 		err := sender.Disconnect()
// 		if err != nil {
// 			p.shared.logger.Println(err)
// 		}
// 		return errors.New("could not find peer in eager or lazy push peers")
// 	}
// 	peer := p.peers[index]
// 	p.lock.Unlock()
// 	p.shared.logger.Printf("%s - received from %s\n", p.shared.self.ID, peer.Node.ID)
// 	p.msgCh <- ReceivedPlumtreeMessage{MsgBytes: msg, Sender: peer}
// 	return nil
// }

// // locked
// func (p *Plumtree) processMsgs() transport.Subscription {
// 	p.shared.logger.Println(p.shared.self.ID, "-", "Message subscription started")
// 	return transport.Subscribe(p.msgCh, func(received ReceivedPlumtreeMessage) {
// 		p.shared.logger.Println(p.shared.self.ID, "-", "Received message in subscription handler", received.MsgBytes)
// 		// p.shared.logger.Println("try lock")
// 		p.lock.Lock()
// 		defer p.lock.Unlock()
// 		msgType := GetMsgType(received.MsgBytes)
// 		handler := p.msgHandlers[msgType]
// 		if handler == nil {
// 			p.shared.logger.Printf("%s - no handler found for message type %v", p.shared.self.ID, msgType)
// 			return
// 		}
// 		payload, err := transport.GetPayload(received.MsgBytes)
// 		if err != nil {
// 			p.shared.logger.Println(p.shared.self.ID, "-", err)
// 			return
// 		}
// 		handler(payload, received.Sender)
// 	})
// }

// locked
func (p *Plumtree) onPeerUp(peer hyparview.Peer) {
	p.shared.logger.Printf("%s - Processing onPeerUp peer: %v\n", p.shared.self.ID, peer.Node.ID)
	// p.shared.logger.Println("try lock")
	p.lock.Lock()
	defer p.lock.Unlock()
	// p.shared.logger.Println(p.shared.self.ID, "-", "peers", p.peers)
	if !slices.ContainsFunc(p.peers, func(p hyparview.Peer) bool {
		return p.Node.ID == peer.Node.ID
	}) {
		p.peers = append(p.peers, peer)
		p.shared.logger.Printf("%s - Added peer %v to peers\n", p.shared.self.ID, peer.Node.ID)
		for _, tree := range p.trees {
			tree.onPeerUp(peer)
		}
	}
	if p.peerUpHandler != nil {
		p.lock.Unlock()
		p.peerUpHandler(peer)
		p.lock.Lock()
	}
}

// locked
func (p *Plumtree) onPeerDown(peer hyparview.Peer) {
	p.shared.logger.Printf("%s - Processing onPeerDown peer: %v\n", p.shared.self.ID, peer.Node.ID)
	// // p.shared.logger.Println("try lock")
	p.lock.Lock()
	defer p.lock.Unlock()
	// p.shared.logger.Println(p.shared.self.ID, "-", "peers", p.peers)
	p.peers = slices.DeleteFunc(p.peers, func(p hyparview.Peer) bool {
		return p.Node.ID == peer.Node.ID
	})
	// p.shared.logger.Println(p.shared.self.ID, "-", "peers", p.peers)
	for _, tree := range p.trees {
		tree.onPeerDown(peer)
	}
	if p.peerDownHandler != nil {
		p.lock.Unlock()
		p.peerDownHandler(peer)
		p.lock.Lock()
	}
}
