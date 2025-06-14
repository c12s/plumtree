package plumtree

import (
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
	shared         *sharedConfig
	metadata       TreeMetadata
	parent         *hyparview.Peer
	eagerPushPeers []hyparview.Peer
	lazyPushPeers  []hyparview.Peer
	receivedMsgs   []PlumtreeCustomMessage
	missingMsgs    map[string][]hyparview.Peer
	lazyQueue      map[string][]PlumtreeCustomMessage
	timers         map[string][]chan struct{}
	stopCh         chan struct{}
	lock           *sync.Mutex
}

func NewTree(shared *sharedConfig, metadata TreeMetadata, peers []hyparview.Peer, lock *sync.Mutex) *Tree {
	t := &Tree{
		shared:         shared,
		metadata:       metadata,
		parent:         nil,
		eagerPushPeers: peers,
		lazyPushPeers:  make([]hyparview.Peer, 0),
		receivedMsgs:   make([]PlumtreeCustomMessage, 0),
		missingMsgs:    make(map[string][]hyparview.Peer),
		lazyQueue:      make(map[string][]PlumtreeCustomMessage),
		timers:         make(map[string][]chan struct{}),
		stopCh:         make(chan struct{}),
		lock:           lock,
	}
	go t.sendAnnouncements()
	return t
}

// locked by caller
func (t *Tree) Gossip(msg PlumtreeCustomMessage) error {
	t.shared.logger.Println(t.shared.self.ID, "-", "Gossiping message")
	t.lock.Unlock()
	proceed := t.shared.gossipMsgHandler(t.metadata, msg.MsgType, msg.Msg, t.shared.self)
	t.lock.Lock()
	if !proceed {
		t.shared.logger.Println(t.shared.self.ID, "-", "Quit broadcast signal from client")
		return nil
	}
	t.eagerPush(msg, t.shared.self)
	t.lazyPush(msg, t.shared.self)
	t.receivedMsgs = append(t.receivedMsgs, msg)
	t.shared.logger.Println(t.shared.self.ID, "-", "Message gossiped successfully")
	return nil
}

// locked by caller
func (t *Tree) SendDirectMsg(msg PlumtreeCustomMessage, receiver transport.Conn) error {
	t.shared.logger.Println(t.shared.self.ID, "-", "Sending direct message")
	err := send(msg, DIRECT_MSG_TYPE, receiver)
	if err != nil {
		t.shared.logger.Println(t.shared.self.ID, "-", "Error sending direct message:", err)
		return err
	}
	t.shared.logger.Println(t.shared.self.ID, "-", "Message sent successfully")
	return nil
}

// locked by caller
func (t *Tree) eagerPush(payload PlumtreeCustomMessage, sender data.Node) {
	t.shared.logger.Println(t.shared.self.ID, "-", "Eager push - sending")
	for _, peer := range t.eagerPushPeers {
		t.shared.logger.Println(t.shared.self.ID, "-", "peer", peer)
		if sender.ID == peer.Node.ID || peer.Conn == nil {
			continue
		}
		t.shared.logger.Printf("%s - Sending gossip msg to peer: %v\n", t.shared.self.ID, peer.Node.ID)
		err := send(payload, GOSSIP_MSG_TYPE, peer.Conn)
		if err != nil {
			t.shared.logger.Println(t.shared.self.ID, "-", "Error sending gossip msg to peer:", err)
		}
	}
}

// locked by caller
func (t *Tree) lazyPush(msg PlumtreeCustomMessage, sender data.Node) {
	t.shared.logger.Println(t.shared.self.ID, "-", "Lazy push - adding to queue")
	for _, peer := range t.lazyPushPeers {
		if sender.ID == peer.Node.ID {
			continue
		}
		t.lazyQueue[peer.Node.ID] = append(t.lazyQueue[peer.Node.ID], msg)
		t.shared.logger.Printf("%s - Added message to lazy queue for peer: %v\n", t.shared.self.ID, peer.Node.ID)
	}
}

// locked
func (t *Tree) sendAnnouncements() {
	t.shared.logger.Println(t.shared.self.ID, "-", "Starting to send announcements periodically")
	ticker := time.NewTicker(time.Duration(t.shared.config.AnnounceInterval) * time.Second)
	for {
		select {
		case <-ticker.C:
			t.lock.Lock()
			t.shared.logger.Println(t.shared.self.ID, "-", "Sending announcements")
			for nodeId, messages := range t.lazyQueue {
				ihaveMsg := PlumtreeIHaveMessage{
					Metadata: t.metadata,
					MsgIds:   make([][]byte, 0),
				}
				for _, msg := range messages {
					ihaveMsg.MsgIds = append(ihaveMsg.MsgIds, msg.MsgId)
				}
				if len(ihaveMsg.MsgIds) == 0 {
					t.shared.logger.Println(t.shared.self.ID, "-", "No messages to send IHave")
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
						t.shared.logger.Println(t.shared.self.ID, "-", "Error sending IHave message to peer:", err)
					}
				}
				t.lazyQueue[nodeId] = []PlumtreeCustomMessage{}
			}
			t.lock.Unlock()
			t.shared.logger.Println(t.shared.self.ID, "-", "Announcements sent")
		case <-t.stopCh:
			t.shared.logger.Println(t.shared.self.ID, "received signal to stop sending announcements")
			return
		}
	}
}

// locked by caller
func (t *Tree) onPeerUp(peer hyparview.Peer) {
	t.shared.logger.Printf("%s - Processing onPeerUp peer: %v\n", t.shared.self.ID, peer.Node.ID)
	t.shared.logger.Println(t.shared.self.ID, "-", "eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
	if !slices.ContainsFunc(t.eagerPushPeers, func(p hyparview.Peer) bool {
		return p.Node.ID == peer.Node.ID
	}) {
		t.eagerPushPeers = append(t.eagerPushPeers, peer)
		t.shared.logger.Printf("%s - Added peer %v to eager push peers\n", t.shared.self.ID, peer.Node.ID)
	}
	t.shared.logger.Println(t.shared.self.ID, "-", "eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
}

// locked by caller
func (t *Tree) onPeerDown(peer hyparview.Peer) {
	t.shared.logger.Printf("%s - Processing onPeerDown peer: %v\n", t.shared.self.ID, peer.Node.ID)
	t.shared.logger.Println(t.shared.self.ID, "-", "eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
	t.eagerPushPeers = slices.DeleteFunc(t.eagerPushPeers, func(p hyparview.Peer) bool {
		return p.Node.ID == peer.Node.ID
	})
	t.lazyPushPeers = slices.DeleteFunc(t.lazyPushPeers, func(p hyparview.Peer) bool {
		return p.Node.ID == peer.Node.ID
	})
	t.shared.logger.Println(t.shared.self.ID, "-", "eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
	for msgId := range t.missingMsgs {
		t.missingMsgs[msgId] = slices.DeleteFunc(t.missingMsgs[msgId], func(p hyparview.Peer) bool {
			return p.Node.ID == peer.Node.ID
		})
	}
	delete(t.lazyQueue, peer.Node.ID)
}
