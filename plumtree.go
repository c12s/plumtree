package plumtree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
)

type Plumtree struct {
	config                 Config
	protocol               MembershipProtocol
	peers                  []hyparview.Peer
	trees                  map[string]*Tree
	deletedTrees           map[string]uint64
	msgCh                  chan ReceivedPlumtreeMessage
	msgSubscription        transport.Subscription
	msgHandlers            map[MessageType]func(msg []byte, sender hyparview.Peer)
	gossipMsgHandler       func(tree TreeMetadata, msgType string, msg []byte, sender data.Node) bool
	directMsgHandler       func(tree TreeMetadata, msgType string, msg []byte, sender data.Node)
	treeConstructedHandler func(tree TreeMetadata)
	treeDestroyedHandler   func(tree TreeMetadata)
	lock                   *sync.Mutex
	logger                 *log.Logger
}

func NewPlumtree(config Config, protocol MembershipProtocol, logger *log.Logger) *Plumtree {
	p := &Plumtree{
		config:                 config,
		protocol:               protocol,
		peers:                  protocol.GetPeers(config.Fanout),
		trees:                  make(map[string]*Tree),
		deletedTrees:           make(map[string]uint64),
		msgCh:                  make(chan ReceivedPlumtreeMessage),
		gossipMsgHandler:       func(m TreeMetadata, t string, b []byte, s data.Node) bool { return true },
		directMsgHandler:       func(m TreeMetadata, t string, b []byte, s data.Node) { return },
		treeConstructedHandler: func(tree TreeMetadata) {},
		treeDestroyedHandler:   func(tree TreeMetadata) {},
		lock:                   new(sync.Mutex),
		logger:                 logger,
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
	p.logger.Println("Plumtree initialized", "peers", p.peers)
	return p
}

func (p *Plumtree) ConstructTree(metadata TreeMetadata) error {
	tree := NewTree(p.config, metadata, p.protocol.Self(), slices.Clone(p.peers), p.gossipMsgHandler, p.directMsgHandler, p.logger)
	p.logger.Println("tree created", metadata.Id)
	p.trees[metadata.Id] = tree
	if p.treeConstructedHandler != nil {
		go p.treeConstructedHandler(tree.metadata)
	}
	return nil
}

func (p *Plumtree) DestroyTree(metadata TreeMetadata) error {
	if _, ok := p.trees[metadata.Id]; !ok {
		return fmt.Errorf("no tree with id=%s found", metadata.Id)
	}
	p.logger.Println("sending destroy gossip msg", metadata)
	p.logger.Println("trees", p.trees, "deleted trees", p.deletedTrees)
	timestamp := uint64(time.Now().Unix())
	delete(p.trees, metadata.Id)
	if p.treeDestroyedHandler != nil {
		go p.treeDestroyedHandler(metadata)
	}
	p.deletedTrees[metadata.Id] = timestamp
	p.logger.Println("trees", p.trees, "deleted trees", p.deletedTrees)
	return nil
}

func (p *Plumtree) Gossip(treeId string, msgType string, msg []byte) error {
	p.logger.Println("Gossiping message")
	if tree, ok := p.trees[treeId]; !ok {
		return fmt.Errorf("no tree with id=%s found", treeId)
	} else {
		self := p.protocol.Self()
		hashFn := fnv.New64()
		ts := time.Now().Unix()
		tsBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(tsBytes, uint64(ts))
		_, err := hashFn.Write(append(append(msg, []byte(self.ID)...), tsBytes...))
		if err != nil {
			p.logger.Println("Error creating hash:", err)
			return err
		}
		msgId := hashFn.Sum(nil)
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

func (p *Plumtree) sendDirectMsg(treeId string, msgType string, msg []byte, receiver transport.Conn) error {
	p.logger.Println("Sending message")
	if tree, ok := p.trees[treeId]; !ok {
		return fmt.Errorf("no tree with id=%s found", treeId)
	} else {
		self := p.protocol.Self()
		hashFn := fnv.New64()
		_, err := hashFn.Write(append(msg, []byte(self.ID)...))
		if err != nil {
			p.logger.Println("Error creating hash:", err)
			return err
		}
		msgId := hashFn.Sum(nil)
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

func (p *Plumtree) SendToParent(treeId string, msgType string, msg []byte) error {
	p.logger.Println("send to parent", p.peers)
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

func (p *Plumtree) HasParent(treeId string) bool {
	p.logger.Println("Get parent", p.peers)
	tree, ok := p.trees[treeId]
	p.logger.Println(tree.parent)
	return ok && tree.parent != nil
}

func (p *Plumtree) GetPeersNum() int {
	p.logger.Println("Get peers num")
	p.logger.Println(p.peers)
	return len(p.peers)
}

func (p *Plumtree) GetChildren(treeId string) ([]data.Node, error) {
	p.logger.Println("Get children", p.peers)
	if tree, ok := p.trees[treeId]; !ok {
		return nil, fmt.Errorf("no tree with id=%s found", treeId)
	} else {
		children := tree.eagerPushPeers
		length := len(children)
		if tree.parent != nil {
			length--
		}
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

func (p *Plumtree) OnGossip(handler func(m TreeMetadata, t string, b []byte, s data.Node) bool) {
	p.gossipMsgHandler = handler
}

func (p *Plumtree) OnDirect(handler func(m TreeMetadata, t string, b []byte, s data.Node)) {
	p.directMsgHandler = handler
}

func (p *Plumtree) OnTreeConstructed(handler func(tree TreeMetadata)) {
	p.treeConstructedHandler = handler
}

func (p *Plumtree) OnTreeDestroyed(handler func(tree TreeMetadata)) {
	p.treeDestroyedHandler = handler
}

func (p *Plumtree) handleMsg(msg []byte, sender transport.Conn) error {
	p.lock.Lock()
	p.logger.Println("Custom message handler invoked")
	p.logger.Println("peers", p.peers)
	p.logger.Println("sender", sender)
	index := slices.IndexFunc(p.peers, func(peer hyparview.Peer) bool {
		return sender != nil && peer.Conn != nil && peer.Conn.GetAddress() == sender.GetAddress()
	})
	if index < 0 {
		p.lock.Unlock()
		return errors.New("could not find peer in eager or lazy push peers")
	}
	peer := p.peers[index]
	p.lock.Unlock()
	p.logger.Printf("%s received from %s\n", p.protocol.Self().ID, peer.Node.ID)
	p.msgCh <- ReceivedPlumtreeMessage{MsgBytes: msg, Sender: peer}
	return nil
}

func (p *Plumtree) processMsgs() transport.Subscription {
	p.logger.Println("Message subscription started")
	return transport.Subscribe(p.msgCh, func(received ReceivedPlumtreeMessage) {
		p.logger.Println("Received message in subscription handler", received.MsgBytes)
		p.lock.Lock()
		defer p.lock.Unlock()
		msgType := GetMsgType(received.MsgBytes)
		handler := p.msgHandlers[msgType]
		if handler == nil {
			p.logger.Printf("no handler found for message type %v", msgType)
			return
		}
		payload, err := transport.GetPayload(received.MsgBytes)
		if err != nil {
			p.logger.Println(err)
			return
		}
		handler(payload, received.Sender)
	})
}

func (p *Plumtree) onPeerUp(peer hyparview.Peer) {
	p.logger.Printf("Processing onPeerUp peer: %v\n", peer.Node.ID)
	p.lock.Lock()
	defer p.lock.Unlock()
	p.logger.Println("peers", p.peers)
	if !slices.ContainsFunc(p.peers, func(p hyparview.Peer) bool {
		return p.Node.ID == peer.Node.ID
	}) {
		p.peers = append(p.peers, peer)
		p.logger.Printf("Added peer %v to peers\n", peer.Node.ID)
		for _, tree := range p.trees {
			tree.onPeerUp(peer)
		}
	}
	p.logger.Println("peers", p.peers)
}

func (p *Plumtree) onPeerDown(peer hyparview.Peer) {
	p.logger.Printf("Processing onPeerDown peer: %v\n", peer.Node.ID)
	p.lock.Lock()
	defer p.lock.Unlock()
	p.logger.Println("peers", p.peers)
	p.peers = slices.DeleteFunc(p.peers, func(p hyparview.Peer) bool {
		return p.Node.ID == peer.Node.ID
	})
	p.logger.Println("peers", p.peers)
	for _, tree := range p.trees {
		tree.onPeerDown(peer)
	}
}
