package plumtree

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
)

func TestTreeConstruction(t *testing.T) {
	const numNodes = 5
	var nodes []*hyparview.HyParView
	port := 8000
	config := hyparview.Config{
		HyParViewConfig: hyparview.HyParViewConfig{
			Fanout:          2,
			PassiveViewSize: 5,
			ARWL:            2,
			PRWL:            2,
			ShuffleInterval: 10,
			Ka:              2,
			Kp:              2,
		},
	}

	for i := 0; i < numNodes; i++ {
		port = port + 1
		config.ContactNodeID = config.NodeID
		config.ContactNodeAddress = config.ListenAddress
		config.NodeID = fmt.Sprintf("node%d", i+1)
		config.ListenAddress = fmt.Sprintf("localhost:%d", port)
		self := data.Node{
			ID:            config.NodeID,
			ListenAddress: config.ListenAddress,
		}
		connManager := transport.NewConnManager(transport.NewTCPConn, transport.AcceptTcpConnsFn(self.ListenAddress))
		node, err := hyparview.NewHyParView(config.HyParViewConfig, self, connManager)
		if err != nil {
			log.Println(err)
		}
		nodes = append(nodes, node)
		time.Sleep(1 * time.Second)
		err = node.Join(config.ContactNodeID, config.ContactNodeAddress)
		if err != nil {
			log.Println(err)
		}
	}

	time.Sleep(10 * time.Second)

	plumtreeConfig := Config{
		Fanout:           10,
		AnnounceInterval: 10,
	}
	trees := []*plumtree{}
	for _, node := range nodes {
		tree := NewPlumtree(plumtreeConfig, node, func(b []byte) bool {
			log.Println(string(b))
			return true
		})
		trees = append(trees, tree)
	}
	err := trees[0].Broadcast([]byte("hello"))
	if err != nil {
		log.Println(err)
	}
	err = trees[0].Broadcast([]byte("hello2"))
	if err != nil {
		log.Println(err)
	}

	time.Sleep(10 * time.Second)

	for _, tree := range trees {
		log.Println("********************")
		log.Println(tree.protocol.Self().ID)
		log.Println(tree.receivedMsgs)
		log.Println(tree.eagerPushPeers)
		log.Println("********************")
	}
}
