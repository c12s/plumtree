package plumtree

import (
	"bytes"
	"slices"
	"time"

	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
)

// locker by caller
func (p *Plumtree) onGossip(msgBytes []byte, sender hyparview.Peer) {
	gossipMsg := PlumtreeCustomMessage{}
	err := transport.Deserialize(msgBytes, &gossipMsg)
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
			p.lock.Unlock()
			go p.treeConstructedHandler(tree.metadata)
			p.lock.Lock()
		}
	}
	tree.onGossip(gossipMsg, sender)
}

// locker by caller
func (p *Plumtree) onDirect(msgBytes []byte, sender hyparview.Peer) {
	directMsg := PlumtreeCustomMessage{}
	err := transport.Deserialize(msgBytes, &directMsg)
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
	err := transport.Deserialize(msgBytes, &pruneMsg)
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
	err := transport.Deserialize(msgBytes, &ihaveMsg)
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
func (p *Plumtree) onForget(msgBytes []byte, sender hyparview.Peer) {
	forgetMsg := PlumtreeForgetMessage{}
	err := transport.Deserialize(msgBytes, &forgetMsg)
	if err != nil {
		p.shared.logger.Println(p.shared.self.ID, "-", "Error unmarshaling forget message:", err)
		return
	}
	if tree, ok := p.trees[forgetMsg.Metadata.Id]; !ok {
		p.shared.logger.Printf("%s - tree with id=%s not found\n", p.shared.self.ID, forgetMsg.Metadata.Id)
	} else {
		tree.onForget(forgetMsg, sender)
	}
}

// locker by caller
func (p *Plumtree) onGraft(msgBytes []byte, sender hyparview.Peer) {
	graftMsg := PlumtreeGraftMessage{}
	err := transport.Deserialize(msgBytes, &graftMsg)
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
		p.lastMsg = time.Now().Unix()
		move(sender, &p.lazyPushPeers, &p.eagerPushPeers)
		p.parent = &sender
		p.receivedMsgs = append(p.receivedMsgs, msg)
		p.lock.Unlock()
		proceed := p.shared.gossipMsgHandler(msg.Metadata, msg.MsgType, msg.Msg, sender)
		p.lock.Lock()
		if !proceed {
			p.forget(msg.MsgId, sender)
		} else {
			msg.Round++
			p.eagerPush(msg, sender.Node)
			p.lazyPush(msg, sender.Node)
		}
	} else {
		p.shared.logger.Printf("%s - Removing peer %s from eager push peers due to duplicate message\n", p.shared.self.ID, sender.Node.ID)
		move(sender, &p.eagerPushPeers, &p.lazyPushPeers)
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
func (p *Tree) onPrune(_ PlumtreePruneMessage, sender hyparview.Peer) {
	p.shared.logger.Printf("%s - Processing prune message from peer: %v\n", p.shared.self.ID, sender.Node.ID)
	p.shared.logger.Println(p.shared.self.ID, "-", "eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
	move(sender, &p.eagerPushPeers, &p.lazyPushPeers)
	p.shared.logger.Println(p.shared.self.ID, "-", "eager push peers", p.eagerPushPeers, "lazy push peers", p.lazyPushPeers)
}

// locker by caller
func (p *Tree) onIHave(msg PlumtreeIHaveMessage, sender hyparview.Peer) {
	p.shared.logger.Printf("%s - Processing IHave message from peer: %v message IDs %v\n", p.shared.self.ID, sender.Node.ID, msg.MsgIds)
	// todo: ??
	move(sender, &p.eagerPushPeers, &p.lazyPushPeers)
	p.lastMsg = time.Now().Unix()
	for _, msgId := range msg.MsgIds {
		if slices.ContainsFunc(p.receivedMsgs, func(received PlumtreeCustomMessage) bool {
			return bytes.Equal([]byte(received.MsgId), msgId)
		}) {
			continue
		}
		if _, ok := p.forgottenMsgs[string(msgId)]; ok {
			continue
		}
		// p.lock.Lock()
		p.missingMsgs[string(msgId)] = append(p.missingMsgs[string(msgId)], sender)
		if _, ok := p.timers[string(msgId)]; !ok {
			p.timers[string(msgId)] = struct{}{}
			go p.setTimer(msgId)
		}
		// p.lock.Unlock()
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

// locker by caller
func (p *Tree) onForget(msg PlumtreeForgetMessage, sender hyparview.Peer) {
	p.shared.logger.Printf("%s - Processing forget message from peer: %v message ID %v\n", p.shared.self.ID, sender.Node.ID, msg.MsgId)
	if _, ok := p.forgottenMsgs[string(msg.MsgId)]; ok {
		// todo: nepotrebno?
		p.shared.logger.Println("already forgot msg with id", msg.MsgId)
		return
	}
	p.forgottenMsgs[string(msg.MsgId)] = true
	p.forget(msg.MsgId, sender)
}

func (p *Tree) setTimer(msgId []byte) {
	p.shared.logger.Println(p.shared.self.ID, "-", "started timer for msg", msgId)
	time.Sleep(time.Duration(p.shared.config.MissingMsgTimeout) * time.Second)
	for !slices.ContainsFunc(p.receivedMsgs, func(msg PlumtreeCustomMessage) bool {
		return bytes.Equal(msg.MsgId, msgId)
	}) && !p.forgottenMsgs[string(msgId)] {
		p.lock.Lock()
		if len(p.missingMsgs[string(msgId)]) == 0 {
			p.lock.Unlock()
			p.shared.logger.Println("no peers to receive missing msg from", msgId)
			break
		}
		first := p.missingMsgs[string(msgId)][0]
		p.missingMsgs[string(msgId)] = slices.DeleteFunc(p.missingMsgs[string(msgId)], func(p hyparview.Peer) bool {
			return p.Node.ID == first.Node.ID
		})
		graftMsg := PlumtreeGraftMessage{
			Metadata: p.metadata,
			MsgId:    msgId,
		}
		err := send(graftMsg, GRAFT_MSG_TYPE, first.Conn)
		if err != nil {
			p.shared.logger.Println(p.shared.self.ID, "-", "Error sending graft message:", err)
		}
		p.lock.Unlock()
		time.Sleep(time.Duration(1 * time.Second))
	}
	p.lock.Lock()
	delete(p.timers, string(msgId))
	delete(p.missingMsgs, string(msgId))
	p.lock.Unlock()
}
