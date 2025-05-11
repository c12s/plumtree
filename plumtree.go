package plumtree

import (
	"encoding/json"
	"hash/fnv"
	"log"
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
	clientMsgHandler func([]byte)
	msgSubscription  transport.Subscription
	lazyQueue        map[string][]PlumtreeGossipMessage
	lazyQueueLock    *sync.Mutex
}

func NewPlumtree(config Config, protocol MembershipProtocol, clientMsgHandler func([]byte)) plumtree {
	p := plumtree{
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
	}
	p.msgSubscription = p.msgSubscribe()
	p.protocol.AddCustomMsgHandler(func(msg []byte, sender transport.Conn) error {
		p.msgCh <- ReceivedPlumtreeMessage{}
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
	payloadSerialized, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	payloadSerialized = append([]byte{byte(GOSSIP_MSG_TYPE)}, payloadSerialized...)
	p.eagerPush(payloadSerialized, self)
	p.lazyPush(payload, self)
	p.msgCh <- ReceivedPlumtreeMessage{
		MsgSerialized: payloadSerialized,
		Sender:        self,
	}
	p.receivedMsgs = append(p.receivedMsgs, msgId)
	return nil
}

func (p *plumtree) eagerPush(payload []byte, sender data.Node) {
	for _, peer := range p.eagerPushPeers {
		if sender.ID == peer.Node.ID || peer.Conn == nil {
			continue
		}
		err := peer.Conn.Send(data.Message{
			Type:    data.CUSTOM,
			Payload: payload,
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
		if len(received.MsgSerialized) == 0 {
			return
		}
		msgType := received.MsgSerialized[0]
		msg := received.MsgSerialized[1:]
		if PlumtreeMessageType(int8(msgType)) == GOSSIP_MSG_TYPE && p.clientMsgHandler != nil {
			gossipMsg := PlumtreeGossipMessage{}
			err := json.Unmarshal(msg, gossipMsg)
			if err != nil {
				log.Println(err)
				return
			}
			p.clientMsgHandler(gossipMsg.Msg)
		}
		// todo prune, ihave, graft
	})
}
