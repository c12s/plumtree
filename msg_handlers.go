package plumtree

import (
	"slices"

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
