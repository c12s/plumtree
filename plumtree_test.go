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
	const numNodes = 20
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

	time.Sleep(2 * time.Second)
	// time.Sleep(10 * time.Second)

	plumtreeConfig := Config{
		Fanout:            500,
		AnnounceInterval:  5,
		MissingMsgTimeout: 3,
		// SecondaryMissingMsgTimeout: 1,
	}
	trees := []*Plumtree{}
	for _, node := range nodes {
		logger := log.New(&lumberjack.Logger{
			Filename: fmt.Sprintf("log/tree_%s.log", node.Self().ID),
		}, node.Self().ID, log.LstdFlags|log.Lshortfile)
		tree := NewPlumtree(plumtreeConfig, node, logger)
		tree.OnGossip(func(m TreeMetadata, t string, b []byte, s data.Node) bool {
			log.Println(m)
			log.Println(s)
			log.Println(t)
			log.Println(string(b))
			return true
		})
		tree.OnTreeConstructed(func(tree TreeMetadata) { logger.Println("tree constructed", tree.Id) })
		tree.OnTreeDestroyed(func(tree TreeMetadata) { logger.Println("tree destroyed", tree.Id) })
		trees = append(trees, tree)
	}
	t1 := TreeMetadata{Id: "t1", Score: 123}
	err := trees[0].ConstructTree(t1)
	if err != nil {
		log.Println(err)
	}
	time.Sleep(2 * time.Second)
	err = trees[0].Broadcast(t1.Id, "custom", []byte("hello"))
	if err != nil {
		log.Println(err)
	}
	time.Sleep(2 * time.Second)
	t2 := TreeMetadata{Id: "t2", Score: 123}
	err = trees[2].ConstructTree(t2)
	if err != nil {
		log.Println(err)
	}
	time.Sleep(2 * time.Second)
	err = trees[0].Broadcast(t1.Id, "custom", []byte("hello2"))
	if err != nil {
		log.Println(err)
	}
	time.Sleep(2 * time.Second)

	drawTrees(trees, "before")

	nodes[1].Leave()
	log.Println("node left")
	nodes[3].Leave()
	log.Println("node left")
	nodes[5].Leave()
	log.Println("node left")
	time.Sleep(2 * time.Second)
	err = trees[2].Broadcast(t2.Id, "custom", []byte("hello3"))
	if err != nil {
		log.Println(err)
	}
	time.Sleep(2 * time.Second)
	err = trees[2].Broadcast(t2.Id, "custom", []byte("hello4"))
	if err != nil {
		log.Println(err)
	}
	time.Sleep(2 * time.Second)
	err = trees[0].Broadcast(t1.Id, "custom", []byte("hello5"))
	if err != nil {
		log.Println(err)
	}
	log.Println("NUM GOROUTINES", runtime.NumGoroutine())
	time.Sleep(10 * time.Second)
	// time.Sleep(60 * time.Second)
	err = trees[0].Broadcast(t1.Id, "custom", []byte("hello6"))
	if err != nil {
		log.Println(err)
	}
	err = trees[2].Broadcast(t2.Id, "custom", []byte("hello7"))
	if err != nil {
		log.Println(err)
	}
	// err = trees[0].DestroyTree(t1)
	// if err != nil {
	// 	log.Println(err)
	// }
	time.Sleep(10 * time.Second)
	// time.Sleep(60 * time.Second)

	for _, tree := range trees {
		log.Println("********************")
		log.Println(tree.protocol.Self().ID)
		for _, t := range tree.trees {
			log.Println("ID", t.metadata.Id)
			log.Println("parent ID", t.parent)
			// for _, msg := range t.receivedMsgs {
			// 	log.Println(msg.MsgId)
			// }
			log.Println("****** peers ******")
			for _, peer := range t.eagerPushPeers {
				log.Println(peer.Node.ID)
			}
		}
		log.Println("********************")
	}

	drawTrees(trees, "after")
}

func drawTrees(trees []*Plumtree, suffix string) {
	graphs := make(map[string]graph.Graph[string, string])
	for _, tree := range trees {
		for _, t := range tree.trees {
			g := graphs[t.metadata.Id]
			if g == nil {
				g = graph.New(graph.StringHash, graph.Directed())
				graphs[t.metadata.Id] = g
			}
		}
	}

	for _, tree := range trees {
		for _, t := range tree.trees {
			g := graphs[t.metadata.Id]
			if g == nil {
				continue
			}
			g.AddVertex(tree.protocol.Self().ID)
		}
	}
	for _, tree := range trees {
		for _, t := range tree.trees {
			g := graphs[t.metadata.Id]
			if g == nil {
				continue
			}
			for _, peer := range t.eagerPushPeers {
				g.AddEdge(tree.protocol.Self().ID, peer.Node.ID)
			}
		}
	}

	for id, g := range graphs {
		fileName := fmt.Sprintf("graphs/tree_%s_%s.gv", id, suffix)
		file, _ := os.Create(fileName)
		_ = draw.DOT(g, file)
		cmd := exec.Command("dot", "-Tsvg", "-O", fileName)
		log.Println(cmd.Args)
		err := cmd.Run()
		if err != nil {
			log.Println("Error executing command:", err)
		}
	}
}
