package plumtree

import (
	"maps"
	"slices"
)

type State struct {
	HyParViewState any
	Peers          []string
	Trees          []TreeState
}

type TreeState struct {
	ID          string
	Parent      string
	EagerPeers  []string
	LazyPeers   []string
	Destroyed   bool
	MissingMsgs []string
}

func (p *Plumtree) GetState() any {
	// p.lock.Lock()
	// defer p.lock.Unlock()
	s := State{
		HyParViewState: p.protocol.GetState(),
		Peers:          make([]string, len(p.peers)),
	}
	for i, p := range p.peers {
		s.Peers[i] = p.Node.ID
	}
	for _, t := range p.trees {
		// t.lock.Lock()
		ts := TreeState{
			ID:          t.metadata.Id,
			EagerPeers:  make([]string, len(t.eagerPushPeers)),
			LazyPeers:   make([]string, len(t.lazyPushPeers)),
			Destroyed:   t.destroyed,
			MissingMsgs: slices.Collect(maps.Keys(t.missingMsgs)),
		}
		if t.parent == nil {
			ts.Parent = "null"
		} else {
			ts.Parent = t.parent.Node.ID
		}
		for i, p := range t.eagerPushPeers {
			ts.EagerPeers[i] = p.Node.ID
		}
		for i, p := range t.lazyPushPeers {
			ts.LazyPeers[i] = p.Node.ID
		}
		s.Trees = append(s.Trees, ts)
		// t.lock.Unlock()
	}
	return s
}
