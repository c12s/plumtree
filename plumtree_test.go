package plumtree

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
	"github.com/dominikbraun/graph"
	"github.com/dominikbraun/graph/draw"
	"github.com/natefinch/lumberjack"
)

func TestTreeConstruction(t *testing.T) {
	const numNodes = 50
	var nodes []*hyparview.HyParView
	port := 8000
	config := hyparview.Config{
		HyParViewConfig: hyparview.HyParViewConfig{
			Fanout:          4,
			PassiveViewSize: 5,
			ARWL:            4,
			PRWL:            2,
			ShuffleInterval: 10,
			Ka:              2,
			Kp:              2,
		},
	}

	for i := 0; i < numNodes; i++ {
		port = port + 1
		config.ContactNodeID = "node1"
		config.ContactNodeAddress = fmt.Sprintf("127.0.0.1:%d", 8001)
		config.NodeID = fmt.Sprintf("node%d", i+1)
		config.ListenAddress = fmt.Sprintf("127.0.0.1:%d", port)
		self := data.Node{
			ID:            config.NodeID,
			ListenAddress: config.ListenAddress,
		}
		connManager := transport.NewConnManager(transport.NewTCPConn, transport.AcceptTcpConnsFn(self.ListenAddress))
		logger := log.New(&lumberjack.Logger{
			Filename: fmt.Sprintf("log/%s.log", config.NodeID),
		}, config.NodeID, log.LstdFlags|log.Lshortfile)
		node, err := hyparview.NewHyParView(config.HyParViewConfig, self, connManager, logger)
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
		Fanout:                     500,
		AnnounceInterval:           5,
		MissingMsgTimeout:          3,
		SecondaryMissingMsgTimeout: 1,
	}
	trees := []*plumtree{}
	for _, node := range nodes {
		logger := log.New(&lumberjack.Logger{
			Filename: fmt.Sprintf("log/tree_%s.log", node.Self().ID),
		}, node.Self().ID, log.LstdFlags|log.Lshortfile)
		tree := NewPlumtree(plumtreeConfig, node, func(b []byte) bool {
			log.Println(string(b))
			return true
		}, logger)
		trees = append(trees, tree)
	}
	err := trees[0].Broadcast([]byte("hello"))
	if err != nil {
		log.Println(err)
	}
	time.Sleep(2 * time.Second)
	err = trees[0].Broadcast([]byte("hello2"))
	if err != nil {
		log.Println(err)
	}
	time.Sleep(2 * time.Second)
	nodes[1].Leave()
	log.Println("node left")
	nodes[3].Leave()
	log.Println("node left")
	nodes[5].Leave()
	log.Println("node left")
	time.Sleep(2 * time.Second)
	err = trees[0].Broadcast([]byte("hello3"))
	if err != nil {
		log.Println(err)
	}
	time.Sleep(2 * time.Second)
	err = trees[0].Broadcast([]byte("hello4"))
	if err != nil {
		log.Println(err)
	}
	time.Sleep(2 * time.Second)
	err = trees[0].Broadcast([]byte("hello5"))
	if err != nil {
		log.Println(err)
	}
	log.Println("NUM GOROUTINES", runtime.NumGoroutine())
	time.Sleep(60 * time.Second)
	err = trees[0].Broadcast([]byte("hello6"))
	if err != nil {
		log.Println(err)
	}
	time.Sleep(60 * time.Second)

	for _, tree := range trees {
		log.Println("********************")
		log.Println(tree.protocol.Self().ID)
		for _, msg := range tree.receivedMsgs {
			log.Println(msg.MsgId)
		}
		log.Println("****** peers ******")
		for _, peer := range tree.eagerPushPeers {
			log.Println(peer.Node.ID)
		}
		log.Println("********************")
	}

	g := graph.New(graph.StringHash, graph.Directed())
	for _, tree := range trees {
		g.AddVertex(tree.protocol.Self().ID)
	}
	for _, tree := range trees {
		for _, peer := range tree.eagerPushPeers {
			g.AddEdge(tree.protocol.Self().ID, peer.Node.ID)
		}
	}
	file, _ := os.Create("tree.gv")
	_ = draw.DOT(g, file)
	cmd := exec.Command("dot", "-Tsvg", "-O", "tree.gv")
	log.Println(cmd.Args)
	err = cmd.Run()
	if err != nil {
		log.Println("Error executing command:", err)
	}
}
