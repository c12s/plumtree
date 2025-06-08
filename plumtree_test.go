package plumtree

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
	"github.com/dominikbraun/graph"
	"github.com/dominikbraun/graph/draw"
)

func TestTreeConstruction(t *testing.T) {
	const numNodes = 20
	trees := []*Plumtree{}

	hvConfig := hyparview.Config{
		Fanout:          4,
		PassiveViewSize: 5,
		ARWL:            2,
		PRWL:            2,
		ShuffleInterval: 5,
		Ka:              3,
		Kp:              3,
	}
	plumtreeConfig := Config{
		Fanout:            50,
		AnnounceInterval:  3,
		MissingMsgTimeout: 3,
	}

	port := 8000
	nodeID := fmt.Sprintf("node%d", 1)
	listenAddress := fmt.Sprintf("127.0.0.1:%d", port+1)

	for i := 0; i < numNodes; i++ {
		contactNodeID := nodeID
		contactNodeAddress := listenAddress
		nodeID = fmt.Sprintf("node%d", i+1)
		listenAddress = fmt.Sprintf("127.0.0.1:%d", port+i+1)

		self := data.Node{
			ID:            nodeID,
			ListenAddress: listenAddress,
		}
		connManager := transport.NewConnManager(transport.NewTCPConn, transport.AcceptTcpConnsFn(self.ListenAddress))
		hvLogFile, err := os.Create(fmt.Sprintf("log/hv_%s.log", nodeID))
		if err != nil {
			log.Fatalf("error opening file: %v", err)
		}
		defer hvLogFile.Close()
		hvLogger := log.New(hvLogFile, "", log.LstdFlags|log.Lshortfile)
		hv, err := hyparview.NewHyParView(hvConfig, self, connManager, hvLogger)
		if err != nil {
			log.Println(err)
		}

		plumtreeLogFile, err := os.Create(fmt.Sprintf("log/pt_%s.log", nodeID))
		if err != nil {
			log.Fatalf("error opening file: %v", err)
		}
		defer plumtreeLogFile.Close()
		ptLogger := log.New(plumtreeLogFile, "", log.LstdFlags|log.Lshortfile)
		tree := NewPlumtree(plumtreeConfig, hv, ptLogger)
		tree.OnGossip(func(m TreeMetadata, t string, b []byte, s data.Node) bool {
			log.Println(m)
			log.Println(s)
			log.Println(t)
			log.Println(string(b))
			return true
		})
		tree.OnTreeConstructed(func(tree TreeMetadata) { log.Println("tree constructed", tree.Id) })
		tree.OnTreeDestroyed(func(tree TreeMetadata) { log.Println("tree destroyed", tree.Id) })
		trees = append(trees, tree)

		time.Sleep(1 * time.Second)

		err = tree.Join(contactNodeID, contactNodeAddress)
		if err != nil {
			log.Println(err)
		}
	}

	t1 := TreeMetadata{Id: "t1", Score: 123}
	err := trees[0].ConstructTree(t1)
	if err != nil {
		log.Println(err)
	}
	time.Sleep(2 * time.Second)
	err = trees[0].Gossip(t1.Id, "custom", []byte("hello"))
	if err != nil {
		log.Println(err)
	}
	time.Sleep(1 * time.Second)
	// t2 := TreeMetadata{Id: "t2", Score: 123}
	// err = trees[2].ConstructTree(t2)
	// if err != nil {
	// 	log.Println(err)
	// }
	// time.Sleep(2 * time.Second)
	err = trees[0].Gossip(t1.Id, "custom", []byte("hello2"))
	if err != nil {
		log.Println(err)
	}
	time.Sleep(1 * time.Second)

	drawTrees(trees, "before")

	for i := 1; i < numNodes; i += 2 {
		trees[i].Leave()
		log.Printf("node %d left\n", i+1)
		time.Sleep(1 * time.Second)
	}
	// err = trees[2].Gossip(t2.Id, "custom", []byte("hello3"))
	// if err != nil {
	// 	log.Println(err)
	// }
	// time.Sleep(2 * time.Second)
	// err = trees[2].Gossip(t2.Id, "custom", []byte("hello4"))
	// if err != nil {
	// 	log.Println(err)
	// }
	// time.Sleep(2 * time.Second)
	err = trees[0].Gossip(t1.Id, "custom", []byte("hello5"))
	if err != nil {
		log.Println(err)
	}
	time.Sleep(1 * time.Second)
	err = trees[0].Gossip(t1.Id, "custom", []byte("hello6"))
	if err != nil {
		log.Println(err)
	}
	// err = trees[2].Gossip(t2.Id, "custom", []byte("hello7"))
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
			log.Println("received msg count", len(t.receivedMsgs))
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
		err := cmd.Run()
		if err != nil {
			log.Println("Error executing command:", err)
		}
	}
}
