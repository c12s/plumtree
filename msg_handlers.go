package plumtree

import (
	"bytes"
	"slices"
	"time"

	"github.com/c12s/hyparview/hyparview"
)

func (p *Plumtree) onGossip(msgBytes []byte, sender hyparview.Peer) {
	gossipMsg := PlumtreeCustomMessage{}
	err := Deserialize(msgBytes, &gossipMsg)
	if err != nil {
		p.logger.Println("Error unmarshaling gossip message:", err)
		return
	}
	tree, ok := p.trees[gossipMsg.Metadata.Id]
	if !ok {
		p.logger.Printf("tree with id=%s not found\n", gossipMsg.Metadata.Id)
		tree = NewTree(p.config, gossipMsg.Metadata, p.protocol.Self(), slices.Clone(p.peers), p.gossipMsgHandler, p.directMsgHandler, p.logger)
		p.logger.Println("tree created", tree.metadata.Id)
		p.trees[tree.metadata.Id] = tree
		if p.treeConstructedHandler != nil {
			go p.treeConstructedHandler(tree.metadata)
		}
	}
	tree.onGossip(gossipMsg, sender)
}

func (p *Plumtree) onDirect(msgBytes []byte, sender hyparview.Peer) {
	directMsg := PlumtreeCustomMessage{}
	err := Deserialize(msgBytes, &directMsg)
	if err != nil {
		p.logger.Println("Error unmarshaling direct message:", err)
		return
	}
	if tree, ok := p.trees[directMsg.Metadata.Id]; !ok {
		p.logger.Printf("tree with id=%s not found\n", directMsg.Metadata.Id)
	} else {
		tree.onGossip(directMsg, sender)
	}
}

func (p *Plumtree) onPrune(msgBytes []byte, sender hyparview.Peer) {
	pruneMsg := PlumtreePruneMessage{}
	err := Deserialize(msgBytes, &pruneMsg)
	if err != nil {
		p.logger.Println("Error unmarshaling prune message:", err)
		return
	}
	if tree, ok := p.trees[pruneMsg.Metadata.Id]; !ok {
		p.logger.Printf("tree with id=%s not found\n", pruneMsg.Metadata.Id)
	} else {
		tree.onPrune(pruneMsg, sender)
	}
}

func (p *Plumtree) onIHave(msgBytes []byte, sender hyparview.Peer) {
	ihaveMsg := PlumtreeIHaveMessage{}
	err := Deserialize(msgBytes, &ihaveMsg)
	if err != nil {
		p.logger.Println("Error unmarshaling IHave message:", err)
		return
	}
	if tree, ok := p.trees[ihaveMsg.Metadata.Id]; !ok {
		p.logger.Printf("tree with id=%s not found\n", ihaveMsg.Metadata.Id)
	} else {
		tree.onIHave(ihaveMsg, sender)
	}
}

func (p *Plumtree) onGraft(msgBytes []byte, sender hyparview.Peer) {
	graftMsg := PlumtreeGraftMessage{}
	err := Deserialize(msgBytes, &graftMsg)
	if err != nil {
		p.logger.Println("Error unmarshaling graft message:", err)
		return
	}
	if tree, ok := p.trees[graftMsg.Metadata.Id]; !ok {
		p.logger.Printf("tree with id=%s not found\n", graftMsg.Metadata.Id)
	} else {
		tree.onGraft(graftMsg, sender)
	}
}

func (t *Tree) onGossip(msg PlumtreeCustomMessage, sender hyparview.Peer) {
	t.logger.Println("Processing gossip message")
	if !slices.ContainsFunc(t.receivedMsgs, func(received PlumtreeCustomMessage) bool {
		return bytes.Equal(msg.MsgId, received.MsgId)
	}) {
		t.logger.Println("message", msg.MsgId, "received for the first time", "add sender to eager push peers", sender.Node)
		t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
		move(sender, &t.lazyPushPeers, &t.eagerPushPeers)
		t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
		t.parent = &sender
		t.receivedMsgs = append(t.receivedMsgs, msg)
		t.stopTimers(msg.MsgId)
		delete(t.timers, string(msg.MsgId))
		delete(t.missingMsgs, string(msg.MsgId))
		proceed := t.gossipMsgHandler(msg.Metadata, msg.MsgType, msg.Msg, sender.Node)
		if !proceed {
			t.logger.Println("Quit broadcast signal from client during gossip")
			return
		}
		msg.Round++
		t.eagerPush(msg, sender.Node)
		t.lazyPush(msg, sender.Node)
	} else {
		t.logger.Printf("Removing peer %s from eager push peers due to duplicate message\n", sender.Node.ID)
		t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
		move(sender, &t.eagerPushPeers, &t.lazyPushPeers)
		t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
		pruneMsg := PlumtreePruneMessage{}
		err := send(pruneMsg, PRUNE_MSG_TYPE, sender.Conn)
		if err != nil {
			t.logger.Println("Error sending prune message:", err)
		}
	}
}

func (t *Tree) onDirect(msg PlumtreeCustomMessage, sender hyparview.Peer) {
	t.logger.Println("Processing direct message")
	t.directMsgHandler(msg.Metadata, msg.MsgType, msg.Msg, sender.Node)
}

func (t *Tree) onPrune(msg PlumtreePruneMessage, sender hyparview.Peer) {
	t.logger.Printf("Processing prune message from peer: %v\n", sender.Node.ID)
	t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
	move(sender, &t.eagerPushPeers, &t.lazyPushPeers)
	t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
}

func (t *Tree) onIHave(msg PlumtreeIHaveMessage, sender hyparview.Peer) {
	t.logger.Printf("Processing IHave message from peer: %v message IDs %v\n", sender.Node.ID, msg.MsgIds)
	for _, msgId := range msg.MsgIds {
		if slices.ContainsFunc(t.receivedMsgs, func(received PlumtreeCustomMessage) bool {
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
	move(sender, &t.lazyPushPeers, &t.eagerPushPeers)
	t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
	msgIndex := slices.IndexFunc(t.receivedMsgs, func(received PlumtreeCustomMessage) bool {
		return bytes.Equal(received.MsgId, msg.MsgId)
	})
	if msgIndex < 0 {
		t.logger.Panicln("could not find in received msgs a missing msg ID", msg.MsgId, "received", t.receivedMsgs)
		return
	}
	missing := t.receivedMsgs[msgIndex]
	err := send(missing, GOSSIP_MSG_TYPE, sender.Conn)
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
			move(first, &t.lazyPushPeers, &t.eagerPushPeers)
			t.logger.Println("eager push peers", t.eagerPushPeers, "lazy push peers", t.lazyPushPeers)
			graftMsg := PlumtreeGraftMessage{
				MsgId: msgId,
			}
			err := send(graftMsg, GRAFT_MSG_TYPE, first.Conn)
			if err != nil {
				t.logger.Println("Error sending graft message:", err)
			}
		case <-quitCh:
			return
		}
	}()
}

func (t *Tree) stopTimers(msgId []byte) {
	if timers, ok := t.timers[string(msgId)]; ok {
		for _, timer := range timers {
			go func() {
				timer <- struct{}{}
			}()
		}
	}
}
