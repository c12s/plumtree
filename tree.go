package plumtree

import (
	"bytes"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
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
	clientMsgHandler func(tree TreeMetadata, msgType string, msg []byte, s data.Node) bool
	receivedMsgs     []PlumtreeGossipMessage
	missingMsgs      map[string][]hyparview.Peer
	lazyQueue        map[int64][]PlumtreeGossipMessage
	timers           map[string][]chan struct{}
	logger           *log.Logger
	lock             *sync.Mutex
}

func NewTree(config Config, metadata TreeMetadata, self data.Node, peers []hyparview.Peer, clientMsgHandler func(tree TreeMetadata, msgType string, msg []byte, s data.Node) bool, logger *log.Logger) *Tree {
	t := &Tree{
		config:           config,
		metadata:         metadata,
		self:             self,
		parent:           nil,
		eagerPushPeers:   peers,
		lazyPushPeers:    make([]hyparview.Peer, 0),
		clientMsgHandler: clientMsgHandler,
		receivedMsgs:     make([]PlumtreeGossipMessage, 0),
		missingMsgs:      make(map[string][]hyparview.Peer),
		lazyQueue:        map[int64][]PlumtreeGossipMessage{},
		timers:           make(map[string][]chan struct{}),
		logger:           logger,
		lock:             new(sync.Mutex),
	}
	go t.sendAnnouncements()
	return t
}

func (t *Tree) Broadcast(msg PlumtreeGossipMessage) error {
	t.logger.Println("Broadcasting message")
	proceed := t.clientMsgHandler(t.metadata, msg.MsgType, msg.Msg, t.self)
	if !proceed {
		t.logger.Println("Quit broadcast signal from client")
		return nil
	}
	msgBytes, err := msg.Serialize()
	if err != nil {
		t.logger.Println("Error serializing payload:", err)
		return err
	}
	t.eagerPush(msgBytes, t.self)
	t.lazyPush(msg, t.self)
	t.receivedMsgs = append(t.receivedMsgs, msg)
	t.logger.Println("Message broadcasted successfully")
	return nil
}

func (t *Tree) Send(msg PlumtreeGossipMessage, receiver *hyparview.Peer) error {
	t.logger.Println("Sending message to one peer")
	msgBytes, err := msg.Serialize()
	if err != nil {
		t.logger.Println("Error serializing payload:", err)
		return err
	}
	err = receiver.Conn.Send(data.Message{
		Type:    data.CUSTOM,
		Payload: msgBytes,
	})
	if err != nil {
		t.logger.Println("Error sending message:", err)
		return err
	}
	t.logger.Println("Message sent successfully")
	return nil
}

func (t *Tree) eagerPush(payload []byte, sender data.Node) {
	t.logger.Println("Eager push - sending")
	for _, peer := range t.eagerPushPeers {
		if sender.ID == peer.Node.ID || peer.Conn == nil {
			continue
		}
		t.logger.Printf("Sending payload to peer: %v\n", peer.Node.ID)
		err := peer.Conn.Send(data.Message{
			Type:    data.CUSTOM,
			Payload: append(payload),
		})
		if err != nil {
			t.logger.Println("Error sending payload to peer:", err)
		}
	}
}

func (t *Tree) lazyPush(msg PlumtreeGossipMessage, sender data.Node) {
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
			ihaveMsgSerialized, err := ihaveMsg.Serialize()
			if err != nil {
				t.logger.Println("Error serializing IHave message:", err)
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
				err := receiver.Conn.Send(data.Message{
					Type:    data.CUSTOM,
					Payload: ihaveMsgSerialized,
				})
				t.logger.Printf("Sent IHave message to peer %v %v\n", nodeId, ihaveMsg.MsgIds)
				if err != nil {
					t.logger.Println("Error sending IHave message to peer:", err)
				}
			}
			t.lazyQueue[nodeId] = []PlumtreeGossipMessage{}
		}
		t.lock.Unlock()
		t.logger.Println("Announcements sent")
	}
}

func (t *Tree) onGossip(msg PlumtreeGossipMessage, sender hyparview.Peer) {
	t.logger.Println("Processing gossip message")
	if !slices.ContainsFunc(t.receivedMsgs, func(received PlumtreeGossipMessage) bool {
		return bytes.Equal(msg.MsgId, received.MsgId)
	}) {
		t.logger.Println("message", msg.MsgId, "received for the first time", "add sender to eager push peers", sender.Node)
		t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
		if !slices.ContainsFunc(t.eagerPushPeers, func(peer hyparview.Peer) bool {
			return peer.Node.ID == sender.Node.ID
		}) {
			t.eagerPushPeers = append(t.eagerPushPeers, sender)
		}
		t.lazyPushPeers = slices.DeleteFunc(t.lazyPushPeers, func(peer hyparview.Peer) bool {
			return peer.Node.ID == sender.Node.ID
		})
		t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
		proceed := t.clientMsgHandler(msg.Metadata, msg.MsgType, msg.Msg, sender.Node)
		if !proceed {
			t.logger.Println("Quit broadcast signal from client during gossip")
			return
		}
		// todo!!!!
		t.parent = &sender
		t.receivedMsgs = append(t.receivedMsgs, msg)
		if timers, ok := t.timers[string(msg.MsgId)]; ok {
			for _, timer := range timers {
				go func() {
					timer <- struct{}{}
				}()
			}
		}
		delete(t.timers, string(msg.MsgId))
		delete(t.missingMsgs, string(msg.MsgId))
		msg.Round++
		payloadSerialized, err := msg.Serialize()
		if err != nil {
			t.logger.Println("Error serializing gossip message:", err)
			return
		}
		t.eagerPush(payloadSerialized, sender.Node)
		t.lazyPush(msg, sender.Node)
	} else {
		t.logger.Printf("Removing peer %s from eager push peers due to duplicate message\n", sender.Node.ID)
		t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
		t.eagerPushPeers = slices.DeleteFunc(t.eagerPushPeers, func(peer hyparview.Peer) bool {
			return peer.Node.ID == sender.Node.ID
		})
		if !slices.ContainsFunc(t.lazyPushPeers, func(peer hyparview.Peer) bool {
			return peer.Node.ID == sender.Node.ID
		}) {
			t.lazyPushPeers = append(t.lazyPushPeers, sender)
		}
		t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
		pruneMsg := PlumtreePruneMessage{}
		pruneMsgSerialized, err := pruneMsg.Serialize()
		if err != nil {
			t.logger.Println("Error serializing prune message:", err)
			return
		}
		err = sender.Conn.Send(data.Message{
			Type:    data.CUSTOM,
			Payload: pruneMsgSerialized,
		})
		if err != nil {
			t.logger.Println("Error sending prune message:", err)
		}
	}
}

func (t *Tree) onPrune(msg PlumtreePruneMessage, sender hyparview.Peer) {
	t.logger.Printf("Processing prune message from peer: %v\n", sender.Node.ID)
	t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
	t.eagerPushPeers = slices.DeleteFunc(t.eagerPushPeers, func(peer hyparview.Peer) bool {
		return peer.Node.ID == sender.Node.ID
	})
	if !slices.ContainsFunc(t.lazyPushPeers, func(peer hyparview.Peer) bool {
		return peer.Node.ID == sender.Node.ID
	}) {
		t.lazyPushPeers = append(t.lazyPushPeers, sender)
		t.logger.Printf("Added peer %v to lazy push peers\n", sender.Node.ID)
	}
	t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
}

func (t *Tree) onIHave(msg PlumtreeIHaveMessage, sender hyparview.Peer) {
	t.logger.Printf("Processing IHave message from peer: %v message IDs %v\n", sender.Node.ID, msg.MsgIds)
	for _, msgId := range msg.MsgIds {
		if slices.ContainsFunc(t.receivedMsgs, func(received PlumtreeGossipMessage) bool {
			return bytes.Equal([]byte(received.MsgId), msgId)
		}) {
			continue
		}
		t.missingMsgs[string(msgId)] = append(t.missingMsgs[string(msgId)], sender)
		if _, ok := t.timers[string(msgId)]; !ok {
			t.setTimer(msgId, t.config.MissingMsgTimeout, t.config.MissingMsgTimeout/2)
		}
	}
}

func (t *Tree) onGraft(msg PlumtreeGraftMessage, sender hyparview.Peer) {
	t.logger.Printf("Processing graft message from peer: %v message ID %v\n", sender.Node.ID, msg.MsgId)
	t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
	t.lazyPushPeers = slices.DeleteFunc(t.lazyPushPeers, func(peer hyparview.Peer) bool {
		return peer.Node.ID == sender.Node.ID
	})
	if !slices.ContainsFunc(t.eagerPushPeers, func(peer hyparview.Peer) bool {
		return peer.Node.ID == sender.Node.ID
	}) {
		t.eagerPushPeers = append(t.eagerPushPeers, sender)
		t.logger.Printf("Added peer %v to eager push peers\n", sender.Node.ID)
	}
	t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
	msgIndex := slices.IndexFunc(t.receivedMsgs, func(received PlumtreeGossipMessage) bool {
		return bytes.Equal(received.MsgId, msg.MsgId)
	})
	if msgIndex < 0 {
		t.logger.Panicln("could not find in received msgs a missing msg ID", msg.MsgId, "received", t.receivedMsgs)
		return
	}
	missing := t.receivedMsgs[msgIndex]
	payloadSerialized, err := missing.Serialize()
	if err != nil {
		t.logger.Println("Error serializing gossip message:", err)
		return
	}
	err = sender.Conn.Send(data.Message{
		Type:    data.CUSTOM,
		Payload: payloadSerialized,
	})
	if err != nil {
		t.logger.Println("Error sending gossip message:", err)
	}
}

func (t *Tree) setTimer(msgId []byte, waitSec, secondaryWaitSec int) {
	go func() {
		t.logger.Println("started timer for msg", msgId)
		quitCh := make(chan struct{})
		t.lock.Lock()
		t.timers[string(msgId)] = append(t.timers[string(msgId)], quitCh)
		t.lock.Unlock()
		select {
		case <-time.NewTicker(time.Duration(waitSec) * time.Second).C:
			t.lock.Lock()
			defer t.lock.Unlock()
			t.timers[string(msgId)] = slices.DeleteFunc(t.timers[string(msgId)], func(ch chan struct{}) bool {
				return ch == quitCh
			})
			t.setTimer(msgId, secondaryWaitSec, secondaryWaitSec)
			t.logger.Println("timer triggered for msg", msgId)
			if len(t.missingMsgs[string(msgId)]) == 0 {
				return
			}
			first := t.missingMsgs[string(msgId)][0]
			t.logger.Println("timer triggered peer selected", first.Node.ID)
			t.missingMsgs[string(msgId)] = slices.Delete(t.missingMsgs[string(msgId)], 0, 1)
			t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
			t.lazyPushPeers = slices.DeleteFunc(t.lazyPushPeers, func(peer hyparview.Peer) bool {
				return peer.Node.ID == first.Node.ID
			})
			if !slices.ContainsFunc(t.eagerPushPeers, func(peer hyparview.Peer) bool {
				return peer.Node.ID == first.Node.ID
			}) {
				t.eagerPushPeers = append(t.eagerPushPeers, first)
				t.logger.Printf("Added peer %v to eager push peers\n", first.Node.ID)
			}
			t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
			graftMsg := PlumtreeGraftMessage{
				MsgId: msgId,
			}
			graftMsgSerialized, err := graftMsg.Serialize()
			if err != nil {
				t.logger.Println("Error serializing graft message:", err)
				return
			}
			err = first.Conn.Send(data.Message{
				Type:    data.CUSTOM,
				Payload: graftMsgSerialized,
			})
			if err != nil {
				t.logger.Println("Error sending graft message:", err)
			}
		case <-quitCh:
			return
		}
	}()
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
