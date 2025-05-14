package plumtree

import (
	"encoding/json"
	"errors"
	"hash/fnv"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
)

type plumtree struct {
	config           Config
	protocol         MembershipProtocol
	eagerPushPeers   []hyparview.Peer
	lazyPushPeers    []hyparview.Peer
	receivedMsgs     map[string]PlumtreeGossipMessage
	missingMsgs      map[string][]hyparview.Peer
	msgCh            chan ReceivedPlumtreeMessage
	clientMsgHandler func([]byte) bool
	msgSubscription  transport.Subscription
	lazyQueue        map[string][]PlumtreeGossipMessage
	lazyQueueLock    *sync.Mutex
	lock             *sync.Mutex
	logger           *log.Logger
	timers           map[string][]chan struct{}
}

func NewPlumtree(config Config, protocol MembershipProtocol, clientMsgHandler func([]byte) bool, logger *log.Logger) *plumtree {
	if clientMsgHandler == nil {
		clientMsgHandler = func(b []byte) bool { return true }
	}
	p := &plumtree{
		config:           config,
		protocol:         protocol,
		eagerPushPeers:   protocol.GetPeers(config.Fanout),
		lazyPushPeers:    make([]hyparview.Peer, 0),
		receivedMsgs:     make(map[string]PlumtreeGossipMessage),
		missingMsgs:      make(map[string][]hyparview.Peer),
		msgCh:            make(chan ReceivedPlumtreeMessage),
		clientMsgHandler: clientMsgHandler,
		lazyQueue:        map[string][]PlumtreeGossipMessage{},
		lazyQueueLock:    new(sync.Mutex),
		lock:             new(sync.Mutex),
		logger:           logger,
	}
	p.msgSubscription = p.msgSubscribe()
	p.protocol.OnPeerUp(p.onPeerUp)
	p.protocol.OnPeerDown(p.onPeerDown)
	p.protocol.AddCustomMsgHandler(func(msg []byte, sender transport.Conn) error {
		p.logger.Println("Custom message handler invoked")
		msgBytes := make([]byte, 10)
		_, err := transport.Deserialize(msg, &msgBytes)
		if err != nil {
			p.logger.Println("Error deserializing message:", err)
			return err
		}
		p.logger.Println("Deserialized message:", msgBytes)
		peers := append(p.eagerPushPeers, p.lazyPushPeers...)
		index := slices.IndexFunc(peers, func(peer hyparview.Peer) bool {
			return sender != nil && peer.Conn != nil && peer.Conn.GetAddress() == sender.GetAddress()
		})
		if index < 0 {
			p.logger.Println("Peer not found in eager or lazy push peers")
			return errors.New("could not find peer in eager or lazy push peers")
		}
		peer := peers[index]
		p.logger.Printf("%s received from %s\n", p.protocol.Self().ID, peer.Node.ID)
		p.msgCh <- ReceivedPlumtreeMessage{MsgSerialized: msgBytes, Sender: peer}
		return nil
	})
	p.logger.Println("plumtree initialized", "eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
	go p.sendAnnouncements()
	return p
}

func (p *plumtree) Broadcast(msg []byte) error {
	p.logger.Println("Broadcasting message")
	self := p.protocol.Self()
	hashFn := fnv.New64()
	_, err := hashFn.Write(append(msg, []byte(self.ID)...))
	if err != nil {
		p.logger.Println("Error creating hash:", err)
		return err
	}
	msgId := hashFn.Sum(nil)
	payload := PlumtreeGossipMessage{
		Msg:   msg,
		MsgId: msgId,
		Round: 0,
	}
	payloadSerialized, err := payload.Serialize()
	if err != nil {
		p.logger.Println("Error serializing payload:", err)
		return err
	}
	proceed := p.clientMsgHandler(msg)
	if !proceed {
		p.logger.Println("Quit broadcast signal from client")
		return nil
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	p.eagerPush(payloadSerialized, self)
	p.lazyPush(payload, self)
	p.receivedMsgs[string(msgId)] = payload
	p.logger.Println("Message broadcasted successfully")
	return nil
}

func (p *plumtree) eagerPush(payload []byte, sender data.Node) {
	p.logger.Println("Eager push - sending")
	for _, peer := range p.eagerPushPeers {
		if sender.ID == peer.Node.ID || peer.Conn == nil {
			continue
		}
		p.logger.Printf("Sending payload to peer: %v\n", peer.Node.ID)
		err := peer.Conn.Send(data.Message{
			Type:    data.CUSTOM,
			Payload: append(payload),
		})
		if err != nil {
			p.logger.Println("Error sending payload to peer:", err)
		}
	}
}

func (p *plumtree) lazyPush(msg PlumtreeGossipMessage, sender data.Node) {
	p.logger.Println("Lazy push - adding to queue")
	for _, peer := range p.lazyPushPeers {
		if sender.ID == peer.Node.ID || peer.Conn == nil {
			continue
		}
		p.lazyQueue[peer.Node.ID] = append(p.lazyQueue[peer.Node.ID], msg)
		p.logger.Printf("Added message to lazy queue for peer: %v\n", peer.Node.ID)
	}
}

func (p *plumtree) sendAnnouncements() {
	p.logger.Println("Starting to send announcements periodically")
	ticker := time.NewTicker(time.Duration(p.config.AnnounceInterval) * time.Second)
	for range ticker.C {
		p.lazyQueueLock.Lock()
		p.lock.Lock()
		for nodeId, messages := range p.lazyQueue {
			if len(messages) == 0 {
				continue
			}
			ihaveMsg := PlumtreeIHaveMessage{
				MsgIds: make([][]byte, 0),
			}
			for _, msg := range messages {
				ihaveMsg.MsgIds = append(ihaveMsg.MsgIds, msg.MsgId)
			}
			ihaveMsgSerialized, err := ihaveMsg.Serialize()
			if err != nil {
				p.logger.Println("Error serializing IHave message:", err)
				continue
			}
			var receiver *hyparview.Peer = nil
			for _, peer := range p.lazyPushPeers {
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
				p.logger.Printf("Sent IHave message to peer %v\n", nodeId)
				if err != nil {
					p.logger.Println("Error sending IHave message to peer:", err)
				}
			}
			p.lazyQueue[nodeId] = []PlumtreeGossipMessage{}
		}
		p.lazyQueueLock.Unlock()
		p.lock.Unlock()
	}
}

func (p *plumtree) msgSubscribe() transport.Subscription {
	p.logger.Println("Message subscription started")
	return transport.Subscribe(p.msgCh, func(received ReceivedPlumtreeMessage) {
		p.logger.Println("Received message in subscription handler")
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
			p.onGossip(gossipMsg, received.Sender)
		} else if PlumtreeMessageType(int8(msgType)) == PRUNE_MSG_TYPE {
			pruneMsg := PlumtreePruneMessage{}
			err := json.Unmarshal(msg, &pruneMsg)
			if err != nil {
				p.logger.Println("Error unmarshaling prune message:", err)
				return
			}
			p.onPrune(pruneMsg, received.Sender)
		} else if PlumtreeMessageType(int8(msgType)) == IHAVE_MSG_TYPE {
			ihaveMsg := PlumtreeIHaveMessage{}
			err := json.Unmarshal(msg, &ihaveMsg)
			if err != nil {
				p.logger.Println("Error unmarshaling IHave message:", err)
				return
			}
			p.onIHave(ihaveMsg, received.Sender)
		} else if PlumtreeMessageType(int8(msgType)) == GRAFT_MSG_TYPE {
			graftMsg := PlumtreeGraftMessage{}
			err := json.Unmarshal(msg, &graftMsg)
			if err != nil {
				p.logger.Println("Error unmarshaling graft message:", err)
				return
			}
			p.onGraft(graftMsg, received.Sender)
		}
	})
}

func (p *plumtree) onGossip(msg PlumtreeGossipMessage, sender hyparview.Peer) {
	p.logger.Println("Processing gossip message")
	if !slices.ContainsFunc(mapKeys(p.receivedMsgs), func(msgId string) bool {
		return msgId == string(msg.MsgId)
	}) {
		proceed := p.clientMsgHandler(msg.Msg)
		if !proceed {
			p.logger.Println("Quit broadcast signal from client during gossip")
			return
		}
		p.receivedMsgs[string(msg.MsgId)] = msg
		if timers, ok := p.timers[string(msg.MsgId)]; ok {
			for _, timer := range timers {
				timer <- struct{}{}
			}
		}
		delete(p.timers, string(msg.MsgId))
		msg.Round++
		payloadSerialized, err := msg.Serialize()
		if err != nil {
			p.logger.Println("Error serializing gossip message:", err)
			return
		}
		p.eagerPush(payloadSerialized, sender.Node)
		p.lazyPush(msg, sender.Node)
		p.logger.Println("message", msg.MsgId, "received for the first time", "add sender to eager push peers", sender.Node)
		p.logger.Println("eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
		if !slices.ContainsFunc(p.eagerPushPeers, func(peer hyparview.Peer) bool {
			return peer.Node.ID == sender.Node.ID
		}) {
			p.eagerPushPeers = append(p.eagerPushPeers, sender)
		}
		p.lazyPushPeers = slices.DeleteFunc(p.lazyPushPeers, func(peer hyparview.Peer) bool {
			return peer.Node.ID == sender.Node.ID
		})
		p.logger.Println("eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
	} else {
		p.logger.Printf("Removing peer %s from eager push peers due to duplicate message\n", sender.Node.ID)
		p.logger.Println("eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
		p.eagerPushPeers = slices.DeleteFunc(p.eagerPushPeers, func(peer hyparview.Peer) bool {
			return peer.Node.ID == sender.Node.ID
		})
		if !slices.ContainsFunc(p.lazyPushPeers, func(peer hyparview.Peer) bool {
			return peer.Node.ID == sender.Node.ID
		}) {
			p.lazyPushPeers = append(p.lazyPushPeers, sender)
		}
		p.logger.Println("eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
		pruneMsg := PlumtreePruneMessage{}
		pruneMsgSerialized, err := pruneMsg.Serialize()
		if err != nil {
			p.logger.Println("Error serializing prune message:", err)
			return
		}
		err = sender.Conn.Send(data.Message{
			Type:    data.CUSTOM,
			Payload: pruneMsgSerialized,
		})
		if err != nil {
			p.logger.Println("Error sending prune message:", err)
		}
	}
}

func (p *plumtree) onPrune(msg PlumtreePruneMessage, sender hyparview.Peer) {
	p.logger.Printf("Processing prune message from peer: %v\n", sender.Node.ID)
	p.logger.Println("eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
	p.eagerPushPeers = slices.DeleteFunc(p.eagerPushPeers, func(peer hyparview.Peer) bool {
		return peer.Node.ID == sender.Node.ID
	})
	if !slices.ContainsFunc(p.lazyPushPeers, func(peer hyparview.Peer) bool {
		return peer.Node.ID == sender.Node.ID
	}) {
		p.lazyPushPeers = append(p.lazyPushPeers, sender)
		p.logger.Printf("Added peer %v to lazy push peers\n", sender.Node.ID)
	}
	p.logger.Println("eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
}

func (p *plumtree) onIHave(msg PlumtreeIHaveMessage, sender hyparview.Peer) {
	p.logger.Printf("Processing IHave message from peer: %v message IDs %v\n", sender.Node.ID, msg.MsgIds)
	for _, msgId := range msg.MsgIds {
		if slices.ContainsFunc(mapKeys(p.receivedMsgs), func(receivedMsgId string) bool {
			return receivedMsgId == string(msgId)
		}) {
			continue
		}
		if _, ok := p.timers[string(msgId)]; !ok {
			p.setTimer(msgId, p.config.MissingMsgTimeout, p.config.SecondaryMissingMsgTimeout)
		}
		p.missingMsgs[string(msgId)] = append(p.missingMsgs[string(msgId)], sender)
	}
}

func (p *plumtree) onGraft(msg PlumtreeGraftMessage, sender hyparview.Peer) {
	p.logger.Printf("Processing graft message from peer: %v message ID %v\n", sender.Node.ID, msg.MsgId)
	p.logger.Println("eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
	p.lazyPushPeers = slices.DeleteFunc(p.lazyPushPeers, func(peer hyparview.Peer) bool {
		return peer.Node.ID == sender.Node.ID
	})
	if !slices.ContainsFunc(p.eagerPushPeers, func(peer hyparview.Peer) bool {
		return peer.Node.ID == sender.Node.ID
	}) {
		p.eagerPushPeers = append(p.eagerPushPeers, sender)
		p.logger.Printf("Added peer %v to eager push peers\n", sender.Node.ID)
	}
	p.logger.Println("eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
	if msg, ok := p.receivedMsgs[string(msg.MsgId)]; ok {
		payloadSerialized, err := msg.Serialize()
		if err != nil {
			p.logger.Println("Error serializing gossip message:", err)
			return
		}
		err = sender.Conn.Send(data.Message{
			Type:    data.CUSTOM,
			Payload: payloadSerialized,
		})
		if err != nil {
			p.logger.Println("Error sending gossip message:", err)
		}
	}
}

func (p *plumtree) setTimer(msgId []byte, waitSec, secondaryWaitSec int) {
	go func() {
		quitCh := make(chan struct{})
		p.timers[string(msgId)] = append(p.timers[string(msgId)], quitCh)
		select {
		case <-time.NewTicker(time.Duration(waitSec) * time.Second).C:
			p.setTimer(msgId, secondaryWaitSec, secondaryWaitSec)
			first := p.missingMsgs[string(msgId)][0]
			p.missingMsgs[string(msgId)] = slices.Delete(p.missingMsgs[string(msgId)], 0, 1)
			p.logger.Println("eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
			p.lazyPushPeers = slices.DeleteFunc(p.lazyPushPeers, func(peer hyparview.Peer) bool {
				return peer.Node.ID == first.Node.ID
			})
			if !slices.ContainsFunc(p.eagerPushPeers, func(peer hyparview.Peer) bool {
				return peer.Node.ID == first.Node.ID
			}) {
				p.eagerPushPeers = append(p.eagerPushPeers, first)
				p.logger.Printf("Added peer %v to eager push peers\n", first.Node.ID)
			}
			p.logger.Println("eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
			graftMsg := PlumtreeGraftMessage{}
			graftMsgSerialized, err := graftMsg.Serialize()
			if err != nil {
				p.logger.Println("Error serializing graft message:", err)
				return
			}
			err = first.Conn.Send(data.Message{
				Type:    data.CUSTOM,
				Payload: graftMsgSerialized,
			})
			if err != nil {
				p.logger.Println("Error sending graft message:", err)
			}
		case <-quitCh:
			return
		}
	}()
}

func (p *plumtree) onPeerUp(peer hyparview.Peer) {
	p.logger.Printf("Processing onPeerUp peer: %v\n", peer.Node.ID)
	p.lock.Lock()
	defer p.lock.Unlock()
	p.logger.Println("eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
	if !slices.ContainsFunc(p.eagerPushPeers, func(p hyparview.Peer) bool {
		return p.Node.ID == peer.Node.ID
	}) {
		p.eagerPushPeers = append(p.eagerPushPeers, peer)
		p.logger.Printf("Added peer %v to eager push peers\n", peer.Node.ID)
	}
	p.logger.Println("eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
}

func (p *plumtree) onPeerDown(peer hyparview.Peer) {
	p.logger.Printf("Processing onPeerDown peer: %v\n", peer.Node.ID)
	p.lock.Lock()
	defer p.lock.Unlock()
	p.logger.Println("eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
	p.eagerPushPeers = slices.DeleteFunc(p.eagerPushPeers, func(p hyparview.Peer) bool {
		return p.Node.ID == peer.Node.ID
	})
	p.lazyPushPeers = slices.DeleteFunc(p.lazyPushPeers, func(p hyparview.Peer) bool {
		return p.Node.ID == peer.Node.ID
	})
	p.logger.Println("eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
	for msgId := range p.missingMsgs {
		p.missingMsgs[msgId] = slices.DeleteFunc(p.missingMsgs[msgId], func(p hyparview.Peer) bool {
			return p.Node.ID == peer.Node.ID
		})
	}
}
