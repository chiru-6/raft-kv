// Command kvnode runs one member of a raft-kv cluster.
//
//	kvnode -id n1 -peers n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003 -data ./data/n1
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/chiru-6/raft-kv/server"
)

func main() {
	id := flag.String("id", "", "this node's ID (must appear in -peers)")
	peersFlag := flag.String("peers", "", "comma-separated id=host:port for every member, including this one")
	listen := flag.String("listen", "", "address to listen on (default: this node's address from -peers)")
	data := flag.String("data", "", "data directory (empty = in-memory, lost on exit)")
	fsync := flag.Bool("fsync", true, "fsync the log before acknowledging writes")
	election := flag.Duration("election-timeout", 300*time.Millisecond, "minimum election timeout")
	verbose := flag.Bool("v", false, "log Raft state changes")
	flag.Parse()

	peers, err := parsePeers(*peersFlag)
	if err != nil || *id == "" {
		fmt.Fprintln(os.Stderr, "usage: kvnode -id n1 -peers n1=host:port,n2=host:port,... [-data dir]")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(2)
	}
	addr := *listen
	if addr == "" {
		addr = strings.TrimPrefix(peers[*id], "http://")
	}

	node, err := server.StartNode(server.NodeConfig{
		ID: *id, Peers: peers, Listen: addr, DataDir: *data, Fsync: *fsync,
		ElectionTimeout: *election, Verbose: *verbose,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%s] serving on %s (data=%q fsync=%v)", *id, node.Addr, *data, *fsync)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("[%s] shutting down", *id)
	node.Close()
}

func parsePeers(s string) (map[string]string, error) {
	peers := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("bad peer %q, want id=host:port", part)
		}
		if !strings.Contains(addr, "://") {
			addr = "http://" + addr
		}
		peers[id] = addr
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("-peers is required")
	}
	return peers, nil
}
