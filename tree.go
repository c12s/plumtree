package plumtree

import (
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
)

type msgRcvd struct {
	time  time.Time
	from  string
	msgId string
}

type ptRcvd struct {
	msgRcvd
	msg PlumtreeCustomMessage
}

type TreeMetadata struct {
	Id    string
	Score int
}

func (t TreeMetadata) NodeID() string {
	return strings.SplitN(t.Id, "_", 2)[1]
}

// func (m TreeMetadata) HasHigherScore(tree TreeMetadata) bool {
// 	return m.Score > tree.Score || m.Id > tree.Id
// }

type Tree struct {
	shared         *sharedConfig
	metadata       TreeMetadata
	parent         *hyparview.Peer
	eagerPushPeers []hyparview.Peer
	lazyPushPeers  []hyparview.Peer
	receivedMsgs   []ptRcvd
	missingMsgs    map[string][]hyparview.Peer
	forgottenMsgs  map[string]msgRcvd
	lazyQueue      map[string][]PlumtreeCustomMessage
	timers         map[string]struct{}
	lock           *sync.Mutex
	destroyed      bool
	lastMsg        int64
	rcvdAll        []msgRcvd
	ihaveAll       []msgRcvd
	activeGraft    map[string]string
}

func NewTree(shared *sharedConfig, metadata TreeMetadata, peers []hyparview.Peer, lock *sync.Mutex) *Tree {
	t := &Tree{
		shared:         shared,
		metadata:       metadata,
		parent:         nil,
		eagerPushPeers: peers,
		lazyPushPeers:  make([]hyparview.Peer, 0),
		receivedMsgs:   make([]ptRcvd, 0),
		missingMsgs:    make(map[string][]hyparview.Peer),
		forgottenMsgs:  make(map[string]msgRcvd),
		lazyQueue:      make(map[string][]PlumtreeCustomMessage),
		timers:         make(map[string]struct{}),
		lock:           lock,
		destroyed:      false,
		rcvdAll:        make([]msgRcvd, 0),
		ihaveAll:       make([]msgRcvd, 0),
		activeGraft:    make(map[string]string),
	}
	go t.sendAnnouncements()
	return t
}

// locked by caller
func (t *Tree) Broadcast(msg PlumtreeCustomMessage) error {
	t.shared.logger.Println(t.shared.self.ID, "-", "Gossiping message")
	t.lastMsg = time.Now().Unix()
	t.lock.Unlock()
	t.shared.gossipMsgHandler(t.metadata, msg.MsgType, msg.Msg, hyparview.Peer{Node: t.shared.self})
	// t.shared.logger.Println("try lock")
	t.lock.Lock()
	t.receivedMsgs = append(t.receivedMsgs, ptRcvd{msgRcvd: msgRcvd{time: time.Now(), from: t.shared.self.ID, msgId: string(msg.MsgId)}, msg: msg})
	msg.Round++
	t.eagerPush(msg, t.shared.self)
	t.lazyPush(msg, t.shared.self)
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
	removePeers := make([]hyparview.Peer, 0)
	for _, peer := range t.eagerPushPeers {
		t.shared.logger.Println(t.shared.self.ID, "-", "peer", peer)
		if sender.ID == peer.Node.ID || peer.Conn == nil {
			continue
		}
		t.shared.logger.Printf("%s - Sending gossip msg to peer: %v\n", t.shared.self.ID, peer.Node.ID)
		err := send(payload, GOSSIP_MSG_TYPE, peer.Conn)
		if err != nil {
			t.shared.logger.Println(t.shared.self.ID, "-", "Error sending gossip msg to peer:", err)
			removePeers = append(removePeers, peer)
		}
	}
	for _, r := range removePeers {
		t.lazyPushPeers = slices.DeleteFunc(t.lazyPushPeers, func(p hyparview.Peer) bool {
			return r.Node.ID == p.Node.ID
		})
		t.eagerPushPeers = slices.DeleteFunc(t.eagerPushPeers, func(p hyparview.Peer) bool {
			return r.Node.ID == p.Node.ID
		})
	}
}

// locked by caller
func (t *Tree) lazyPush(msg PlumtreeCustomMessage, sender data.Node) {
	t.shared.logger.Println(t.shared.self.ID, "-", "Lazy push - sending")
	// removePeers := make([]hyparview.Peer, 0)
	for _, peer := range t.lazyPushPeers {
		// if i >= t.shared.config.Fanout-1 {
		// 	break
		// }
		t.shared.logger.Println(t.shared.self.ID, "-", "peer", peer)
		if sender.ID == peer.Node.ID || peer.Conn == nil {
			continue
		}
		// ihaveMsg := PlumtreeIHaveMessage{
		// 	Metadata: t.metadata,
		// 	MsgIds:   [][]byte{msg.MsgId},
		// }
		// t.shared.logger.Printf("%s - Sending ihave msg to peer: %v\n", t.shared.self.ID, peer.Node.ID)
		// err := send(ihaveMsg, IHAVE_MSG_TYPE, peer.Conn)
		// if err != nil {
		// 	removePeers = append(removePeers, peer)
		// 	t.shared.logger.Println(t.shared.self.ID, "-", "Error sending IHave message to peer:", err)
		// }
		t.lazyQueue[peer.Node.ID] = append(t.lazyQueue[peer.Node.ID], msg)
		t.shared.logger.Printf("%s - Added message to lazy queue for peer: %v\n", t.shared.self.ID, peer.Node.ID)
	}
	// for _, r := range removePeers {
	// 	t.lazyPushPeers = slices.DeleteFunc(t.lazyPushPeers, func(p hyparview.Peer) bool {
	// 		return r.Node.ID == p.Node.ID
	// 	})
	// 	t.eagerPushPeers = slices.DeleteFunc(t.eagerPushPeers, func(p hyparview.Peer) bool {
	// 		return r.Node.ID == p.Node.ID
	// 	})
	// }
}

// locked
func (t *Tree) sendAnnouncements() {
	t.shared.logger.Println(t.shared.self.ID, "-", "Starting to send announcements periodically")
	ticker := time.NewTicker(time.Duration(t.shared.config.AnnounceInterval) * time.Second)
	for range ticker.C {
		t.lock.Lock()
		if t.destroyed {
			t.lock.Unlock()
			continue
		}
		removePeers := make([]hyparview.Peer, 0)
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
					removePeers = append(removePeers, *receiver)
					t.shared.logger.Println(t.shared.self.ID, "-", "Error sending IHave message to peer:", err)
				}
			}
			t.lazyQueue[nodeId] = []PlumtreeCustomMessage{}
		}
		for _, r := range removePeers {
			t.lazyPushPeers = slices.DeleteFunc(t.lazyPushPeers, func(p hyparview.Peer) bool {
				return r.Node.ID == p.Node.ID
			})
			t.eagerPushPeers = slices.DeleteFunc(t.eagerPushPeers, func(p hyparview.Peer) bool {
				return r.Node.ID == p.Node.ID
			})
		}
		t.lock.Unlock()
		t.shared.logger.Println(t.shared.self.ID, "-", "Announcements sent")
	}
	// for {
	// 	select {
	// 	case <-ticker.C:
	// 		t.lock.Lock()
	// 		t.shared.logger.Println(t.shared.self.ID, "-", "Sending announcements")
	// 		for nodeId, messages := range t.lazyQueue {
	// 			ihaveMsg := PlumtreeIHaveMessage{
	// 				Metadata: t.metadata,
	// 				MsgIds:   make([][]byte, 0),
	// 			}
	// 			for _, msg := range messages {
	// 				ihaveMsg.MsgIds = append(ihaveMsg.MsgIds, msg.MsgId)
	// 			}
	// 			if len(ihaveMsg.MsgIds) == 0 {
	// 				t.shared.logger.Println(t.shared.self.ID, "-", "No messages to send IHave")
	// 				continue
	// 			}
	// 			var receiver *hyparview.Peer = nil
	// 			for _, peer := range t.lazyPushPeers {
	// 				if peer.Node.ID != nodeId || peer.Conn == nil {
	// 					continue
	// 				}
	// 				receiver = &peer
	// 				break
	// 			}
	// 			if receiver != nil {
	// 				err := send(ihaveMsg, IHAVE_MSG_TYPE, receiver.Conn)
	// 				if err != nil {
	// 					t.shared.logger.Println(t.shared.self.ID, "-", "Error sending IHave message to peer:", err)
	// 				}
	// 			}
	// 			t.lazyQueue[nodeId] = []PlumtreeCustomMessage{}
	// 		}
	// 		t.lock.Unlock()
	// 		t.shared.logger.Println(t.shared.self.ID, "-", "Announcements sent")
	// 	case <-t.stopCh:
	// 		t.shared.logger.Println(t.shared.self.ID, "received signal to stop sending announcements")
	// 		return
	// 	}
	// }
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
	if t.parent != nil && peer.Node.ID == t.parent.Node.ID {
		t.parent = nil
	}
	// delete(t.lazyQueue, peer.Node.ID)
}

func (t *Tree) forget(msgId []byte, sender hyparview.Peer) {
	t.shared.logger.Println(t.shared.self.ID, "-", "Forget msg - sending")
	removePeers := make([]hyparview.Peer, 0)
	for _, peer := range t.eagerPushPeers {
		// t.shared.logger.Println(t.shared.self.ID, "-", "peer", peer)
		if sender.Node.ID == peer.Node.ID || peer.Conn == nil {
			continue
		}
		t.shared.logger.Printf("%s - Sending forget msg id=%v to peer: %v\n", t.shared.self.ID, msgId, peer.Node.ID)
		err := send(PlumtreeForgetMessage{
			Metadata: t.metadata,
			MsgId:    msgId,
		}, FORGET_MSG_TYPE, peer.Conn)
		if err != nil {
			t.shared.logger.Println(t.shared.self.ID, "-", "Error sending forget msg to peer:", err)
			removePeers = append(removePeers, peer)
		}
	}
	for _, r := range removePeers {
		t.lazyPushPeers = slices.DeleteFunc(t.lazyPushPeers, func(p hyparview.Peer) bool {
			return r.Node.ID == p.Node.ID
		})
		t.eagerPushPeers = slices.DeleteFunc(t.eagerPushPeers, func(p hyparview.Peer) bool {
			return r.Node.ID == p.Node.ID
		})
	}
}

// locked by caller
func (t *Tree) bestFitForEagerPeer(candidates []hyparview.Peer) *hyparview.Peer {
	// 1. Group recent messages by peer
	ihaveByPeer := make(map[string][]msgRcvd)
	for _, msg := range t.ihaveAll {
		if msg.time.Add(60 * time.Second).Before(time.Now()) {
			continue
		}
		if !slices.ContainsFunc(candidates, func(p hyparview.Peer) bool {
			return p.Node.ID == msg.from
		}) {
			continue
		}
		ihaveByPeer[msg.from] = append(ihaveByPeer[msg.from], msg)
	}

	// 2. Select peers with the maximum number of messages
	maxPeers := make([]string, 0)
	maxCount := 0
	for peer, msgs := range ihaveByPeer {
		if len(msgs) > maxCount {
			maxCount = len(msgs)
			maxPeers = []string{peer}
		} else if len(msgs) == maxCount {
			maxPeers = append(maxPeers, peer)
		}
	}
	if len(maxPeers) == 0 {
		return nil
	}

	// 3. Build map[msgId][]msgRcvd for selected peers
	msgsByID := make(map[string][]msgRcvd)
	for _, peer := range maxPeers {
		for _, msg := range ihaveByPeer[peer] {
			msgsByID[msg.msgId] = append(msgsByID[msg.msgId], msg)
		}
	}

	// 4. Compute per-peer latency totals
	latencySum := make(map[string]time.Duration)
	latencyCount := make(map[string]int)

	for _, msgs := range msgsByID {
		// find first arrival for this msgId
		firstArrival := msgs[0].time
		for _, m := range msgs[1:] {
			if m.time.Before(firstArrival) {
				firstArrival = m.time
			}
		}
		// compute latency for each peer that received this msg
		for _, m := range msgs {
			lat := m.time.Sub(firstArrival)
			latencySum[m.from] += lat
			latencyCount[m.from]++
		}
	}

	// 5. Compute average latency per peer and find the best (lowest)
	bestPeer := ""
	bestAvg := time.Duration(1<<63 - 1) // max int64

	for _, peer := range maxPeers {
		if latencyCount[peer] == 0 {
			continue
		}
		avg := latencySum[peer] / time.Duration(latencyCount[peer])
		if avg < bestAvg {
			bestAvg = avg
			bestPeer = peer
		}
	}

	// 6. Return matching hyparview.Peer
	for _, p := range candidates {
		if p.Node.ID == bestPeer {
			return &p
		}
	}
	return nil
}
