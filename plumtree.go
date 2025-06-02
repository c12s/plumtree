package plumtree

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"slices"
	"strconv"
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
	clientMsgHandler       func(tree TreeMetadata, msgType string, msg []byte, sender data.Node) bool
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
		clientMsgHandler:       func(m TreeMetadata, t string, b []byte, s data.Node) bool { return true },
		treeConstructedHandler: func(tree TreeMetadata) {},
		treeDestroyedHandler:   func(tree TreeMetadata) {},
		lock:                   new(sync.Mutex),
		logger:                 logger,
	}
	p.msgSubscription = p.msgSubscribe()
	p.protocol.OnPeerUp(p.onPeerUp)
	p.protocol.OnPeerDown(p.onPeerDown)
	p.protocol.AddCustomMsgHandler(func(msg []byte, sender transport.Conn) error {
		p.lock.Lock()
		p.logger.Println("Custom message handler invoked")
		msgBytes := make([]byte, 10)
		_, err := transport.Deserialize(msg, &msgBytes)
		if err != nil {
			p.lock.Unlock()
			p.logger.Println("Error deserializing message:", err)
			return err
		}
		p.logger.Println("Deserialized message:", msgBytes)
		index := slices.IndexFunc(p.peers, func(peer hyparview.Peer) bool {
			return sender != nil && peer.Conn != nil && peer.Conn.GetAddress() == sender.GetAddress()
		})
		if index < 0 {
			p.lock.Unlock()
			p.logger.Println("Peer not found in eager or push peers")
			return errors.New("could not find peer in eager or push peers")
		}
		peer := p.peers[index]
		p.lock.Unlock()
		p.logger.Printf("%s received from %s\n", p.protocol.Self().ID, peer.Node.ID)
		p.msgCh <- ReceivedPlumtreeMessage{MsgSerialized: msgBytes, Sender: peer}
		return nil
	})
	p.logger.Println("Plumtree initialized", "peers", p.peers)
	return p
}

func (p *Plumtree) OnMessage(handler func(m TreeMetadata, t string, b []byte, s data.Node) bool) {
	p.clientMsgHandler = handler
}

func (p *Plumtree) OnTreeConstructed(handler func(tree TreeMetadata)) {
	p.treeConstructedHandler = handler
}

func (p *Plumtree) OnTreeDestroyed(handler func(tree TreeMetadata)) {
	p.treeDestroyedHandler = handler
}

func (p *Plumtree) ConstructTree(metadata TreeMetadata) error {
	tree := NewTree(p.config, metadata, p.protocol.Self(), p.peers, p.clientMsgHandler, p.logger)
	p.trees[metadata.Id] = tree
	if p.treeConstructedHandler != nil {
		go p.treeConstructedHandler(tree.metadata)
	}
	return nil
	// timestamp := make([]byte, 8)
	// binary.LittleEndian.PutUint64(timestamp, uint64(time.Now().Unix()))
	// return p.Broadcast(tree.metadata.Id, "init", timestamp)
}

func (p *Plumtree) DestroyTree(metadata TreeMetadata) error {
	if _, ok := p.trees[metadata.Id]; !ok {
		return fmt.Errorf("no tree with id=%s found", metadata.Id)
	}
	p.logger.Println("sending destroy gossip msg", metadata)
	p.logger.Println("trees", p.trees, "deleted trees", p.deletedTrees)
	timestamp := uint64(time.Now().Unix())
	// timestampBytes := make([]byte, 8)
	// binary.LittleEndian.PutUint64(timestampBytes, timestamp)
	// err := p.Broadcast(metadata.Id, "destroy", timestampBytes)
	// if err != nil {
	// 	p.logger.Println("error while broadcasting destroy msg", err)
	// }
	// p.lock.Lock()
	delete(p.trees, metadata.Id)
	if p.treeDestroyedHandler != nil {
		go p.treeDestroyedHandler(metadata)
	}
	p.deletedTrees[metadata.Id] = timestamp
	p.logger.Println("trees", p.trees, "deleted trees", p.deletedTrees)
	// p.lock.Unlock()
	return nil
}

func (p *Plumtree) Broadcast(treeId string, msgType string, msg []byte) error {
	p.logger.Println("Broadcasting message")
	// p.lock.Lock()
	// defer p.lock.Unlock()
	if tree, ok := p.trees[treeId]; !ok {
		return fmt.Errorf("no tree with id=%s found", treeId)
	} else {
		self := p.protocol.Self()
		hashFn := fnv.New64()
		_, err := hashFn.Write(append(msg, []byte(strconv.Itoa(int(self.ID)))...))
		if err != nil {
			p.logger.Println("Error creating hash:", err)
			return err
		}
		msgId := hashFn.Sum(nil)
		payload := PlumtreeGossipMessage{
			Metadata: tree.metadata,
			MsgType:  msgType,
			MsgId:    msgId,
			Msg:      msg,
			Round:    0,
		}
		return tree.Broadcast(payload)
	}
}

func (p *Plumtree) Send(treeId string, msgType string, msg []byte, receiver *hyparview.Peer) error {
	p.logger.Println("Sending message")
	// p.lock.Lock()
	// defer p.lock.Unlock()
	if tree, ok := p.trees[treeId]; !ok {
		return fmt.Errorf("no tree with id=%s found", treeId)
	} else {
		self := p.protocol.Self()
		hashFn := fnv.New64()
		_, err := hashFn.Write(append(msg, []byte(strconv.Itoa(int(self.ID)))...))
		if err != nil {
			p.logger.Println("Error creating hash:", err)
			return err
		}
		msgId := hashFn.Sum(nil)
		payload := PlumtreeGossipMessage{
			Metadata: tree.metadata,
			MsgType:  msgType,
			MsgId:    msgId,
			Msg:      msg,
			Round:    0,
		}
		return tree.Send(payload, receiver)
	}
}

func (p *Plumtree) GetParent(treeId string) (*hyparview.Peer, error) {
	p.logger.Println("Get parent")
	if tree, ok := p.trees[treeId]; !ok {
		return nil, fmt.Errorf("no tree with id=%s found", treeId)
	} else {
		return tree.parent, nil
	}
}

func (p *Plumtree) GetPeers() []hyparview.Peer {
	p.logger.Println("Get peers")
	return slices.Clone(p.peers)
}

func (p *Plumtree) GetChildren(treeId string) ([]hyparview.Peer, error) {
	p.logger.Println("Get children")
	if tree, ok := p.trees[treeId]; !ok {
		return nil, fmt.Errorf("no tree with id=%s found", treeId)
	} else {
		if tree.parent == nil {
			return slices.Clone(tree.eagerPushPeers), nil
		}
		return slices.DeleteFunc(tree.eagerPushPeers, func(peer hyparview.Peer) bool {
			return peer.Node.ID == tree.parent.Node.ID
		}), nil
	}
}

func (p *Plumtree) msgSubscribe() transport.Subscription {
	p.logger.Println("Message subscription started")
	return transport.Subscribe(p.msgCh, func(received ReceivedPlumtreeMessage) {
		p.logger.Println("Received message in subscription handler", received.MsgSerialized)
		p.lock.Lock()
		defer p.lock.Unlock()
		if len(received.MsgSerialized) == 0 {
			p.logger.Println("Received empty message")
			return
		}
		msgType := received.MsgSerialized[0]
		msg := received.MsgSerialized[1:]
		if PlumtreeMessageType(int8(msgType)) == GOSSIP_MSG_TYPE {
			gossipMsg := PlumtreeGossipMessage{}
			err := json.Unmarshal(msg, &gossipMsg)
			if err != nil {
				p.logger.Println("Error unmarshaling gossip message:", err)
				return
			}
			if tree, ok := p.trees[gossipMsg.Metadata.Id]; !ok {
				p.logger.Printf("tree with id=%s not found\n", gossipMsg.Metadata.Id)
				// deletedTimestamp, wasDeleted := p.deletedTrees[gossipMsg.Metadata.Id]
				// reinit := false
				// if wasDeleted && gossipMsg.MsgType == "init" {
				// 	initTimestamp := binary.LittleEndian.Uint64(gossipMsg.Msg)
				// 	reinit = initTimestamp > deletedTimestamp
				// }
				// if !wasDeleted || reinit {
				// 	delete(p.deletedTrees, gossipMsg.Metadata.Id)
				tree := NewTree(p.config, gossipMsg.Metadata, p.protocol.Self(), p.peers, p.clientMsgHandler, p.logger)
				p.trees[tree.metadata.Id] = tree
				if p.treeConstructedHandler != nil {
					go p.treeConstructedHandler(tree.metadata)
				}
				tree.onGossip(gossipMsg, received.Sender)
				// }
			} else {
				// if string(gossipMsg.MsgType) == "destroy" {
				// 	p.logger.Println("received destroy gossip msg", gossipMsg)
				// 	p.logger.Println("trees", p.trees, "deleted trees", p.deletedTrees)
				// 	delete(p.trees, gossipMsg.Metadata.Id)
				// 	if p.treeDestroyedHandler != nil {
				// 		p.treeDestroyedHandler(tree.metadata)
				// 	}
				// 	deleteTimestamp := binary.LittleEndian.Uint64(gossipMsg.Msg)
				// 	p.deletedTrees[gossipMsg.Metadata.Id] = deleteTimestamp
				// 	p.logger.Println("trees", p.trees, "deleted trees", p.deletedTrees)
				// 	err = tree.Broadcast(gossipMsg)
				// 	if err != nil {
				// 		p.logger.Println("error broadcasting destroy msg", gossipMsg)
				// 	}
				// } else {
				tree.onGossip(gossipMsg, received.Sender)
				// }
			}
		} else if PlumtreeMessageType(int8(msgType)) == PRUNE_MSG_TYPE {
			pruneMsg := PlumtreePruneMessage{}
			err := json.Unmarshal(msg, &pruneMsg)
			if err != nil {
				p.logger.Println("Error unmarshaling prune message:", err)
				return
			}
			if tree, ok := p.trees[pruneMsg.Metadata.Id]; !ok {
				p.logger.Printf("tree with id=%s not found\n", pruneMsg.Metadata.Id)
			} else {
				tree.onPrune(pruneMsg, received.Sender)
			}
		} else if PlumtreeMessageType(int8(msgType)) == IHAVE_MSG_TYPE {
			ihaveMsg := PlumtreeIHaveMessage{}
			err := json.Unmarshal(msg, &ihaveMsg)
			if err != nil {
				p.logger.Println("Error unmarshaling IHave message:", err)
				return
			}
			if tree, ok := p.trees[ihaveMsg.Metadata.Id]; !ok {
				p.logger.Printf("tree with id=%s not found\n", ihaveMsg.Metadata.Id)
			} else {
				tree.onIHave(ihaveMsg, received.Sender)
			}
		} else if PlumtreeMessageType(int8(msgType)) == GRAFT_MSG_TYPE {
			graftMsg := PlumtreeGraftMessage{}
			err := json.Unmarshal(msg, &graftMsg)
			if err != nil {
				p.logger.Println("Error unmarshaling graft message:", err)
				return
			}
			if tree, ok := p.trees[graftMsg.Metadata.Id]; !ok {
				p.logger.Printf("tree with id=%s not found\n", graftMsg.Metadata.Id)
			} else {
				tree.onGraft(graftMsg, received.Sender)
			}
		} else {
			p.logger.Println("message type unknown:", msgType)
		}
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
