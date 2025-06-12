package plumtree

import (
	"bytes"
	"slices"
	"time"

	"github.com/c12s/hyparview/hyparview"
)

// locker by caller
func (p *Plumtree) onGossip(msgBytes []byte, sender hyparview.Peer) {
	gossipMsg := PlumtreeCustomMessage{}
	err := Deserialize(msgBytes, &gossipMsg)
	if err != nil {
		p.shared.logger.Println(p.shared.self.ID, "-", "Error unmarshaling gossip message:", err)
		return
	}
	tree, ok := p.trees[gossipMsg.Metadata.Id]
	if !ok {
		p.shared.logger.Printf("%s - tree with id=%s not found\n", p.shared.self.ID, gossipMsg.Metadata.Id)
		tree = NewTree(p.shared, gossipMsg.Metadata, slices.Clone(p.peers), p.lock)
		p.shared.logger.Println(p.shared.self.ID, "-", "tree created", tree.metadata.Id)
		p.trees[tree.metadata.Id] = tree
		if p.treeConstructedHandler != nil {
			// p.lock.Unlock()
			go p.treeConstructedHandler(tree.metadata)
			// p.shared.logger.Println("try lock")
			// p.lock.Lock()
		}
	}
	tree.onGossip(gossipMsg, sender)
}

// locker by caller
func (p *Plumtree) onDirect(msgBytes []byte, sender hyparview.Peer) {
	directMsg := PlumtreeCustomMessage{}
	err := Deserialize(msgBytes, &directMsg)
	if err != nil {
		p.shared.logger.Println(p.shared.self.ID, "-", "Error unmarshaling direct message:", err)
		return
	}
	if tree, ok := p.trees[directMsg.Metadata.Id]; !ok {
		p.shared.logger.Printf("%s - tree with id=%s not found\n", p.shared.self.ID, directMsg.Metadata.Id)
	} else {
		tree.onDirect(directMsg, sender)
	}
}

// locker by caller
func (p *Plumtree) onPrune(msgBytes []byte, sender hyparview.Peer) {
	pruneMsg := PlumtreePruneMessage{}
	err := Deserialize(msgBytes, &pruneMsg)
	if err != nil {
		p.shared.logger.Println(p.shared.self.ID, "-", "Error unmarshaling prune message:", err)
		return
	}
	if tree, ok := p.trees[pruneMsg.Metadata.Id]; !ok {
		p.shared.logger.Printf("%s - tree with id=%s not found\n", p.shared.self.ID, pruneMsg.Metadata.Id)
	} else {
		tree.onPrune(pruneMsg, sender)
	}
}

// locker by caller
func (p *Plumtree) onIHave(msgBytes []byte, sender hyparview.Peer) {
	ihaveMsg := PlumtreeIHaveMessage{}
	err := Deserialize(msgBytes, &ihaveMsg)
	if err != nil {
		p.shared.logger.Println(p.shared.self.ID, "-", "Error unmarshaling IHave message:", err)
		return
	}
	if tree, ok := p.trees[ihaveMsg.Metadata.Id]; !ok {
		p.shared.logger.Printf("%s - tree with id=%s not found\n", p.shared.self.ID, ihaveMsg.Metadata.Id)
	} else {
		tree.onIHave(ihaveMsg, sender)
	}
}

// locker by caller
func (p *Plumtree) onGraft(msgBytes []byte, sender hyparview.Peer) {
	graftMsg := PlumtreeGraftMessage{}
	err := Deserialize(msgBytes, &graftMsg)
	if err != nil {
		p.shared.logger.Println(p.shared.self.ID, "-", "Error unmarshaling graft message:", err)
		return
	}
	if tree, ok := p.trees[graftMsg.Metadata.Id]; !ok {
		p.shared.logger.Printf("%s - tree with id=%s not found\n", p.shared.self.ID, graftMsg.Metadata.Id)
	} else {
		tree.onGraft(graftMsg, sender)
	}
}

// locker by caller
func (p *Tree) onGossip(msg PlumtreeCustomMessage, sender hyparview.Peer) {
	p.shared.logger.Println(p.shared.self.ID, "-", "Processing gossip message")
	if !slices.ContainsFunc(p.receivedMsgs, func(received PlumtreeCustomMessage) bool {
		return bytes.Equal(msg.MsgId, received.MsgId)
	}) {
		p.shared.logger.Println(p.shared.self.ID, "-", "message", msg.MsgId, "received for the first time", "add sender to eager push peers", sender.Node)
		p.shared.logger.Println(p.shared.self.ID, "-", "eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
		move(sender, &p.lazyPushPeers, &p.eagerPushPeers)
		p.shared.logger.Println(p.shared.self.ID, "-", "eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
		p.parent = &sender
		p.receivedMsgs = append(p.receivedMsgs, msg)
		p.stopTimers(msg.MsgId)
		delete(p.timers, string(msg.MsgId))
		delete(p.missingMsgs, string(msg.MsgId))
		p.lock.Unlock()
		proceed := p.shared.gossipMsgHandler(msg.Metadata, msg.MsgType, msg.Msg, sender.Node)
		p.shared.logger.Println("try lock")
		p.lock.Lock()
		if !proceed {
			p.shared.logger.Println(p.shared.self.ID, "-", "Quit broadcast signal from client during gossip")
			return
		}
		msg.Round++
		p.eagerPush(msg, sender.Node)
		p.lazyPush(msg, sender.Node)
	} else {
		p.shared.logger.Printf("%s - Removing peer %s from eager push peers due to duplicate message\n", p.shared.self.ID, sender.Node.ID)
		p.shared.logger.Println(p.shared.self.ID, "-", "eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
		move(sender, &p.eagerPushPeers, &p.lazyPushPeers)
		p.shared.logger.Println(p.shared.self.ID, "-", "eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
		pruneMsg := PlumtreePruneMessage{Metadata: msg.Metadata}
		err := send(pruneMsg, PRUNE_MSG_TYPE, sender.Conn)
		if err != nil {
			p.shared.logger.Println(p.shared.self.ID, "-", "Error sending prune message:", err)
		}
	}
}

// locker by caller
func (p *Tree) onDirect(msg PlumtreeCustomMessage, sender hyparview.Peer) {
	p.shared.logger.Println(p.shared.self.ID, "-", "Processing direct message")
	go p.shared.directMsgHandler(msg.Metadata, msg.MsgType, msg.Msg, sender.Node)
}

// locker by caller
func (p *Tree) onPrune(msg PlumtreePruneMessage, sender hyparview.Peer) {
	p.shared.logger.Printf("%s - Processing prune message from peer: %v\n", p.shared.self.ID, sender.Node.ID)
	p.shared.logger.Println(p.shared.self.ID, "-", "eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
	move(sender, &p.eagerPushPeers, &p.lazyPushPeers)
	p.shared.logger.Println(p.shared.self.ID, "-", "eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
}

// locker by caller
func (p *Tree) onIHave(msg PlumtreeIHaveMessage, sender hyparview.Peer) {
	p.shared.logger.Printf("%s - Processing IHave message from peer: %v message IDs %v\n", p.shared.self.ID, sender.Node.ID, msg.MsgIds)
	for _, msgId := range msg.MsgIds {
		if slices.ContainsFunc(p.receivedMsgs, func(received PlumtreeCustomMessage) bool {
			return bytes.Equal([]byte(received.MsgId), msgId)
		}) {
			continue
		}
		p.missingMsgs[string(msgId)] = append(p.missingMsgs[string(msgId)], sender)
		if _, ok := p.timers[string(msgId)]; !ok {
			p.setTimer(msgId)
		}
	}
}

// locker by caller
func (p *Tree) onGraft(msg PlumtreeGraftMessage, sender hyparview.Peer) {
	p.shared.logger.Printf("%s - Processing graft message from peer: %v message ID %v\n", p.shared.self.ID, sender.Node.ID, msg.MsgId)
	p.shared.logger.Println(p.shared.self.ID, "-", "eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
	move(sender, &p.lazyPushPeers, &p.eagerPushPeers)
	p.shared.logger.Println(p.shared.self.ID, "-", "eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
	msgIndex := slices.IndexFunc(p.receivedMsgs, func(received PlumtreeCustomMessage) bool {
		return bytes.Equal(received.MsgId, msg.MsgId)
	})
	if msgIndex < 0 {
		p.shared.logger.Println("could not find in received msgs a missing msg ID", msg.MsgId, "received", p.receivedMsgs)
		return
	}
	missing := p.receivedMsgs[msgIndex]
	err := send(missing, GOSSIP_MSG_TYPE, sender.Conn)
	if err != nil {
		p.shared.logger.Println(p.shared.self.ID, "-", "Error sending missing message:", err)
	}
}

// locked
func (p *Tree) setTimer(msgId []byte) {
	go func() {
		p.shared.logger.Println(p.shared.self.ID, "-", p.shared.self.ID, "-", "started timer for msg", msgId)
		quitCh := make(chan struct{})
		p.shared.logger.Println("try lock")
		p.lock.Lock()
		p.timers[string(msgId)] = append(p.timers[string(msgId)], quitCh)
		p.lock.Unlock()
		p.shared.logger.Println("missing msg timeout", p.shared.config.MissingMsgTimeout)
		select {
		case <-time.NewTicker(time.Duration(p.shared.config.MissingMsgTimeout) * time.Second).C:
			p.shared.logger.Println("try lock")
			p.lock.Lock()
			defer p.lock.Unlock()
			p.timers[string(msgId)] = slices.DeleteFunc(p.timers[string(msgId)], func(ch chan struct{}) bool {
				return ch == quitCh
			})
			if len(p.missingMsgs[string(msgId)]) == 0 {
				p.shared.logger.Println("no peers to receive missing msg from", msgId)
				return
			}
			p.setTimer(msgId)
			p.shared.logger.Println(p.shared.self.ID, "-", p.shared.self.ID, "-", "timer triggered for msg", msgId)
			p.shared.logger.Println(p.shared.self.ID, "-", "missing msgs", p.missingMsgs)
			first := p.missingMsgs[string(msgId)][0]
			p.shared.logger.Println(p.shared.self.ID, "-", "timer triggered peer selected", first.Node.ID)
			p.missingMsgs[string(msgId)] = slices.Delete(p.missingMsgs[string(msgId)], 0, 1)
			p.shared.logger.Println(p.shared.self.ID, "-", "eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
			move(first, &p.lazyPushPeers, &p.eagerPushPeers)
			p.shared.logger.Println(p.shared.self.ID, "-", "eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
			graftMsg := PlumtreeGraftMessage{
				Metadata: p.metadata,
				MsgId:    msgId,
			}
			err := send(graftMsg, GRAFT_MSG_TYPE, first.Conn)
			if err != nil {
				p.shared.logger.Println(p.shared.self.ID, "-", "Error sending graft message:", err)
			}
		case <-quitCh:
			return
		}
	}()
}

// locked by caller
func (p *Tree) stopTimers(msgId []byte) {
	if timers, ok := p.timers[string(msgId)]; ok {
		for _, timer := range timers {
			go func() {
				timer <- struct{}{}
			}()
		}
	}
}
