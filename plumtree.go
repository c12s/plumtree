package plumtree

import (
	"bytes"
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
	receivedMsgs     [][]byte
	missingMsgs      [][]byte
	msgCh            chan ReceivedPlumtreeMessage
	clientMsgHandler func([]byte) bool
	msgSubscription  transport.Subscription
	lazyQueue        map[string][]PlumtreeGossipMessage
	lazyQueueLock    *sync.Mutex
	lock             *sync.Mutex
}

func NewPlumtree(config Config, protocol MembershipProtocol, clientMsgHandler func([]byte) bool) *plumtree {
	if clientMsgHandler == nil {
		clientMsgHandler = func(b []byte) bool { return true }
	}
	p := &plumtree{
		config:           config,
		protocol:         protocol,
		eagerPushPeers:   protocol.GetPeers(config.Fanout),
		lazyPushPeers:    make([]hyparview.Peer, 0),
		receivedMsgs:     make([][]byte, 0),
		missingMsgs:      make([][]byte, 0),
		msgCh:            make(chan ReceivedPlumtreeMessage),
		clientMsgHandler: clientMsgHandler,
		lazyQueue:        map[string][]PlumtreeGossipMessage{},
		lazyQueueLock:    new(sync.Mutex),
		lock:             new(sync.Mutex),
	}
	p.msgSubscription = p.msgSubscribe()
	p.protocol.AddCustomMsgHandler(func(msg []byte, sender transport.Conn) error {
		// log.Println("plumtree received msg")
		msgBytes := make([]byte, 10)
		_, err := transport.Deserialize(msg, &msgBytes)
		if err != nil {
			return err
		}
		// log.Println(msgBytes)
		peers := append(p.eagerPushPeers, p.lazyPushPeers...)
		index := slices.IndexFunc(peers, func(peer hyparview.Peer) bool {
			return sender != nil && peer.Conn != nil && peer.Conn.GetAddress() == sender.GetAddress()
		})
		if index < 0 {
			return errors.New("could not find peer in eager or push peers")
		}
		peer := peers[index]
		log.Printf("%s received from %s\n", p.protocol.Self().ID, peer.Node.ID)
		p.msgCh <- ReceivedPlumtreeMessage{MsgSerialized: msgBytes, Sender: peer}
		return nil
	})
	go p.sendAnnouncements()
	return p
}

func (p *plumtree) Broadcast(msg []byte) error {
	self := p.protocol.Self()
	hashFn := fnv.New64()
	_, err := hashFn.Write(append(msg, []byte(self.ID)...))
	if err != nil {
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
		return err
	}
	proceed := p.clientMsgHandler(msg)
	if !proceed {
		log.Println("quit broadcast signal from client")
		return nil
	}
	p.eagerPush(payloadSerialized, self)
	p.lazyPush(payload, self)
	p.receivedMsgs = append(p.receivedMsgs, msgId)
	return nil
}

func (p *plumtree) eagerPush(payload []byte, sender data.Node) {
	// log.Println(p.protocol.Self().ID)
	// log.Println("eager push - sending")
	// log.Println(payload)
	for _, peer := range p.eagerPushPeers {
		if sender.ID == peer.Node.ID || peer.Conn == nil {
			continue
		}
		// log.Println(peer)
		err := peer.Conn.Send(data.Message{
			Type:    data.CUSTOM,
			Payload: append(payload),
		})
		if err != nil {
			log.Println(err)
		}
	}
}

func (p *plumtree) lazyPush(msg PlumtreeGossipMessage, sender data.Node) {
	p.lazyQueueLock.Lock()
	defer p.lazyQueueLock.Unlock()
	for _, peer := range p.eagerPushPeers {
		if sender.ID == peer.Node.ID || peer.Conn == nil {
			continue
		}
		p.lazyQueue[peer.Node.ID] = append(p.lazyQueue[peer.Node.ID], msg)
	}
}

func (p *plumtree) sendAnnouncements() {
	ticker := time.NewTicker(time.Duration(p.config.AnnounceInterval) * time.Second)
	for range ticker.C {
		p.lazyQueueLock.Lock()
		for nodeId, messages := range p.lazyQueue {
			ihaveMsg := PlumtreeIHaveMessage{
				MsgIds: make([][]byte, 0),
			}
			for _, msg := range messages {
				ihaveMsg.MsgIds = append(ihaveMsg.MsgIds, msg.MsgId)
			}
			ihaveMsgSerialized, err := json.Marshal(ihaveMsg)
			if err != nil {
				log.Println(err)
				continue
			}
			ihaveMsgSerialized = append([]byte{byte(IHAVE_MSG_TYPE)}, ihaveMsgSerialized...)
			var receiver *hyparview.Peer = nil
			for _, peer := range p.eagerPushPeers {
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
				if err != nil {
					log.Println(err)
				}
			}
			p.lazyQueue[nodeId] = []PlumtreeGossipMessage{}
		}
		p.lazyQueueLock.Unlock()
	}
}

func (p *plumtree) msgSubscribe() transport.Subscription {
	return transport.Subscribe(p.msgCh, func(received ReceivedPlumtreeMessage) {
		// log.Println("plumtree msg subscribe handler")
		// log.Println(received.MsgSerialized)
		// log.Println(string(received.MsgSerialized))
		p.lock.Lock()
		defer p.lock.Unlock()
		if len(received.MsgSerialized) == 0 {
			return
		}
		msgType := received.MsgSerialized[0]
		msg := received.MsgSerialized[1:]
		if PlumtreeMessageType(int8(msgType)) == GOSSIP_MSG_TYPE {
			gossipMsg := PlumtreeGossipMessage{}
			err := json.Unmarshal(msg, &gossipMsg)
			if err != nil {
				log.Println(err)
				return
			}
			p.onGossip(gossipMsg, received.Sender)
		} else if PlumtreeMessageType(int8(msgType)) == PRUNE_MSG_TYPE {
			pruneMsg := PlumtreePruneMessage{}
			err := json.Unmarshal(msg, &pruneMsg)
			if err != nil {
				log.Println(err)
				return
			}
			p.onPrune(pruneMsg, received.Sender)
		}
		// todo ihave, graft
	})
}

func (p *plumtree) onGossip(msg PlumtreeGossipMessage, sender hyparview.Peer) {
	// log.Println("plumtree on gossip")
	log.Println(msg.MsgId)
	if !slices.ContainsFunc(p.receivedMsgs, func(msgId []byte) bool {
		return bytes.Equal(msg.MsgId, msgId)
	}) {
		proceed := p.clientMsgHandler(msg.Msg)
		if !proceed {
			log.Println("quit broadcast signal from client")
			return
		}
		p.receivedMsgs = append(p.receivedMsgs, msg.MsgId)
		// todo: ako postoji tajmer za ovu poruku, otkazi ga
		msg.Round++
		payloadSerialized, err := msg.Serialize()
		if err != nil {
			log.Println(err)
			return
		}
		p.eagerPush(payloadSerialized, sender.Node)
		p.lazyPush(msg, sender.Node)
		if !slices.ContainsFunc(p.eagerPushPeers, func(peer hyparview.Peer) bool {
			return peer.Node.ID == sender.Node.ID
		}) {
			p.eagerPushPeers = append(p.eagerPushPeers, sender)
		}
		p.lazyPushPeers = slices.DeleteFunc(p.lazyPushPeers, func(peer hyparview.Peer) bool {
			return peer.Node.ID == sender.Node.ID
		})
		// todo: call optimize
	} else {
		log.Printf("%s removes %s from eager push peers\n", p.protocol.Self().ID, sender.Node.ID)
		p.eagerPushPeers = slices.DeleteFunc(p.eagerPushPeers, func(peer hyparview.Peer) bool {
			return peer.Node.ID == sender.Node.ID
		})
		if !slices.ContainsFunc(p.lazyPushPeers, func(peer hyparview.Peer) bool {
			return peer.Node.ID == sender.Node.ID
		}) {
			p.lazyPushPeers = append(p.lazyPushPeers, sender)
		}
		pruneMsg := PlumtreePruneMessage{}
		pruneMsgSerialized, err := pruneMsg.Serialize()
		if err != nil {
			log.Println(err)
			return
		}
		err = sender.Conn.Send(data.Message{
			Type:    data.CUSTOM,
			Payload: pruneMsgSerialized,
		})
		if err != nil {
			log.Println(err)
		}
	}
}

func (p *plumtree) onPrune(msg PlumtreePruneMessage, sender hyparview.Peer) {
	p.eagerPushPeers = slices.DeleteFunc(p.eagerPushPeers, func(peer hyparview.Peer) bool {
		return peer.Node.ID == sender.Node.ID
	})
	if !slices.ContainsFunc(p.lazyPushPeers, func(peer hyparview.Peer) bool {
		return peer.Node.ID == sender.Node.ID
	}) {
		p.lazyPushPeers = append(p.lazyPushPeers, sender)
	}
}
