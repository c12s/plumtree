package plumtree

import (
	"log"
	"slices"
	"sync"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
)

type TreeMetadata struct {
	Id    string
	Score float64
}

func (m TreeMetadata) HasHigherScore(tree TreeMetadata) bool {
	return m.Score > tree.Score || m.Id > tree.Id
}

type Tree struct {
	config           Config
	metadata         TreeMetadata
	self             data.Node
	parent           *hyparview.Peer
	eagerPushPeers   []hyparview.Peer
	lazyPushPeers    []hyparview.Peer
	gossipMsgHandler func(tree TreeMetadata, msgType string, msg []byte, s data.Node) bool
	directMsgHandler func(tree TreeMetadata, msgType string, msg []byte, s data.Node)
	receivedMsgs     []PlumtreeCustomMessage
	missingMsgs      map[string][]hyparview.Peer
	lazyQueue        map[string][]PlumtreeCustomMessage
	timers           map[string][]chan struct{}
	logger           *log.Logger
	lock             *sync.Mutex
}

func NewTree(config Config, metadata TreeMetadata, self data.Node, peers []hyparview.Peer, gossipMsgHandler func(tree TreeMetadata, msgType string, msg []byte, s data.Node) bool, directMsgHandler func(tree TreeMetadata, msgType string, msg []byte, s data.Node), logger *log.Logger) *Tree {
	t := &Tree{
		config:           config,
		metadata:         metadata,
		self:             self,
		parent:           nil,
		eagerPushPeers:   peers,
		lazyPushPeers:    make([]hyparview.Peer, 0),
		gossipMsgHandler: gossipMsgHandler,
		directMsgHandler: directMsgHandler,
		receivedMsgs:     make([]PlumtreeCustomMessage, 0),
		missingMsgs:      make(map[string][]hyparview.Peer),
		lazyQueue:        make(map[string][]PlumtreeCustomMessage),
		timers:           make(map[string][]chan struct{}),
		logger:           logger,
		lock:             new(sync.Mutex),
	}
	go t.sendAnnouncements()
	return t
}

func (t *Tree) Gossip(msg PlumtreeCustomMessage) error {
	t.logger.Println("Gossiping message")
	proceed := t.gossipMsgHandler(t.metadata, msg.MsgType, msg.Msg, t.self)
	if !proceed {
		t.logger.Println("Quit broadcast signal from client")
		return nil
	}
	t.eagerPush(msg, t.self)
	t.lazyPush(msg, t.self)
	t.receivedMsgs = append(t.receivedMsgs, msg)
	t.logger.Println("Message gossiped successfully")
	return nil
}

func (t *Tree) SendDirectMsg(msg PlumtreeCustomMessage, receiver transport.Conn) error {
	t.logger.Println("Sending direct message")
	err := send(msg, DIRECT_MSG_TYPE, receiver)
	if err != nil {
		t.logger.Println("Error sending direct message:", err)
		return err
	}
	t.logger.Println("Message sent successfully")
	return nil
}

func (t *Tree) eagerPush(payload PlumtreeCustomMessage, sender data.Node) {
	t.logger.Println("Eager push - sending")
	for _, peer := range t.eagerPushPeers {
		t.logger.Println("peer", peer)
		if sender.ID == peer.Node.ID || peer.Conn == nil {
			continue
		}
		t.logger.Printf("Sending gossip msg to peer: %v\n", peer.Node.ID)
		err := send(payload, GOSSIP_MSG_TYPE, peer.Conn)
		if err != nil {
			t.logger.Println("Error sending gossip msg to peer:", err)
		}
	}
}

func (t *Tree) lazyPush(msg PlumtreeCustomMessage, sender data.Node) {
	t.logger.Println("Lazy push - adding to queue")
	for _, peer := range t.lazyPushPeers {
		if sender.ID == peer.Node.ID || peer.Conn == nil {
			continue
		}
		t.lazyQueue[peer.Node.ID] = append(t.lazyQueue[peer.Node.ID], msg)
		t.logger.Printf("Added message to lazy queue for peer: %v\n", peer.Node.ID)
	}
}

func (t *Tree) sendAnnouncements() {
	t.logger.Println("Starting to send announcements periodically")
	ticker := time.NewTicker(time.Duration(t.config.AnnounceInterval) * time.Second)
	for range ticker.C {
		t.lock.Lock()
		t.logger.Println("Sending announcements")
		for nodeId, messages := range t.lazyQueue {
			ihaveMsg := PlumtreeIHaveMessage{
				MsgIds: make([][]byte, 0),
			}
			for _, msg := range messages {
				ihaveMsg.MsgIds = append(ihaveMsg.MsgIds, msg.MsgId)
			}
			if len(ihaveMsg.MsgIds) == 0 {
				t.logger.Println("No messages to send IHave")
				continue
			}
			var receiver *hyparview.Peer = nil
			for _, peer := range t.lazyPushPeers {
				if peer.Node.ID != nodeId || peer.Conn == nil {
					continue
				}
				receiver = &peer
				break
			}
			if receiver != nil {
				err := send(ihaveMsg, IHAVE_MSG_TYPE, receiver.Conn)
				if err != nil {
					t.logger.Println("Error sending IHave message to peer:", err)
				}
			}
			t.lazyQueue[nodeId] = []PlumtreeCustomMessage{}
		}
		t.lock.Unlock()
		t.logger.Println("Announcements sent")
	}
}

func (t *Tree) onPeerUp(peer hyparview.Peer) {
	t.logger.Printf("Processing onPeerUp peer: %v\n", peer.Node.ID)
	t.lock.Lock()
	defer t.lock.Unlock()
	t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
	if !slices.ContainsFunc(t.eagerPushPeers, func(p hyparview.Peer) bool {
		return p.Node.ID == peer.Node.ID
	}) {
		t.eagerPushPeers = append(t.eagerPushPeers, peer)
		t.logger.Printf("Added peer %v to eager push peers\n", peer.Node.ID)
	}
	t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
}

func (t *Tree) onPeerDown(peer hyparview.Peer) {
	t.logger.Printf("Processing onPeerUp peer: %v\n", peer.Node.ID)
	t.lock.Lock()
	defer t.lock.Unlock()
	t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
	t.eagerPushPeers = slices.DeleteFunc(t.eagerPushPeers, func(p hyparview.Peer) bool {
		return p.Node.ID == peer.Node.ID
	})
	t.lazyPushPeers = slices.DeleteFunc(t.lazyPushPeers, func(p hyparview.Peer) bool {
		return p.Node.ID == peer.Node.ID
	})
	t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
	for msgId := range t.missingMsgs {
		t.missingMsgs[msgId] = slices.DeleteFunc(t.missingMsgs[msgId], func(p hyparview.Peer) bool {
			return p.Node.ID == peer.Node.ID
		})
	}
	delete(t.lazyQueue, peer.Node.ID)
}
