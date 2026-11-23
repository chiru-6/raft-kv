// Command kvbench measures throughput and latency of a raft-kv cluster.
//
// Against a running cluster:
//
//	kvbench -cluster 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 -clients 32 -duration 15s
//
// Or let it start a local in-process cluster (real HTTP on loopback, real
// files on disk):
//
//	kvbench -inproc 3 -clients 32 -duration 15s -fsync=true
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/chiru-6/raft-kv/client"
	"github.com/chiru-6/raft-kv/server"
)

func main() {
	cluster := flag.String("cluster", "", "comma-separated node addresses of a running cluster")
	inproc := flag.Int("inproc", 0, "start an in-process cluster of this many nodes instead")
	fsync := flag.Bool("fsync", true, "with -inproc: fsync the log on every write")
	clients := flag.Int("clients", 16, "concurrent clients")
	duration := flag.Duration("duration", 10*time.Second, "how long to run")
	readRatio := flag.Float64("reads", 0.5, "fraction of operations that are reads (0..1)")
	stale := flag.Bool("stale", false, "use stale (local) reads instead of linearizable reads")
	keys := flag.Int("keys", 1000, "size of the key space")
	valueSize := flag.Int("value-size", 128, "value size in bytes")
	markdown := flag.Bool("md", false, "print a markdown table row")
	flag.Parse()

	var addrs []string
	switch {
	case *inproc > 0:
		var stop func()
		addrs, stop = startInProc(*inproc, *fsync)
		defer stop()
	case *cluster != "":
		addrs = strings.Split(*cluster, ",")
	default:
		log.Fatal("give -cluster or -inproc")
	}

	value := strings.Repeat("x", *valueSize)
	// Warm up: make sure a leader exists and keys are populated.
	warm := client.New(addrs)
	for i := 0; i < min(*keys, 200); i++ { // the rest get created during the run
		if err := warm.Put(context.Background(), fmt.Sprintf("key-%d", i), value); err != nil {
			log.Fatalf("warm-up: %v", err)
		}
	}

	type sample struct {
		read bool
		d    time.Duration
		err  bool
	}
	results := make([][]sample, *clients)
	var wg sync.WaitGroup
	start := time.Now()
	deadline := start.Add(*duration)
	for w := 0; w < *clients; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := client.New(addrs)
			rng := rand.New(rand.NewSource(int64(w)))
			for time.Now().Before(deadline) {
				key := fmt.Sprintf("key-%d", rng.Intn(*keys))
				read := rng.Float64() < *readRatio
				t0 := time.Now()
				var err error
				switch {
				case read && *stale:
					_, _, err = c.StaleGet(context.Background(), key)
				case read:
					_, _, err = c.Get(context.Background(), key)
				default:
					err = c.Put(context.Background(), key, value)
				}
				results[w] = append(results[w], sample{read, time.Since(t0), err != nil})
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	var reads, writes []time.Duration
	errs := 0
	for _, rs := range results {
		for _, s := range rs {
			switch {
			case s.err:
				errs++
			case s.read:
				reads = append(reads, s.d)
			default:
				writes = append(writes, s.d)
			}
		}
	}
	total := len(reads) + len(writes)
	tput := float64(total) / elapsed.Seconds()
	nodes := len(addrs)

	if *markdown {
		fmt.Printf("| %d | %d | %.0f%% | %v | %.0f | %s | %s | %s | %d |\n",
			nodes, *clients, *readRatio*100, *fsync, tput,
			pct(writes, 50), pct(writes, 99), pct(reads, 99), errs)
		return
	}
	fmt.Printf("nodes=%d clients=%d duration=%v reads=%.0f%% stale=%v value=%dB fsync=%v\n",
		nodes, *clients, elapsed.Round(time.Millisecond), *readRatio*100, *stale, *valueSize, *fsync)
	fmt.Printf("throughput: %.0f ops/s (%d ops, %d errors)\n", tput, total, errs)
	fmt.Printf("writes: n=%-7d p50=%-8s p95=%-8s p99=%s\n", len(writes), pct(writes, 50), pct(writes, 95), pct(writes, 99))
	fmt.Printf("reads:  n=%-7d p50=%-8s p95=%-8s p99=%s\n", len(reads), pct(reads, 50), pct(reads, 95), pct(reads, 99))
}

func pct(ds []time.Duration, p int) string {
	if len(ds) == 0 {
		return "-"
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	i := len(ds) * p / 100
	if i >= len(ds) {
		i = len(ds) - 1
	}
	return ds[i].Round(10 * time.Microsecond).String()
}

func startInProc(n int, fsync bool) ([]string, func()) {
	dir, err := os.MkdirTemp("", "kvbench-")
	if err != nil {
		log.Fatal(err)
	}
	peers := map[string]string{}
	lns := map[string]net.Listener{}
	var addrs []string
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			log.Fatal(err)
		}
		lns[id] = ln
		peers[id] = "http://" + ln.Addr().String()
		addrs = append(addrs, ln.Addr().String())
	}
	var nodes []*server.Node
	for id, ln := range lns {
		node, err := server.StartNode(server.NodeConfig{
			ID: id, Peers: peers, Listener: ln,
			DataDir: filepath.Join(dir, id), Fsync: fsync,
		})
		if err != nil {
			log.Fatal(err)
		}
		nodes = append(nodes, node)
	}
	return addrs, func() {
		for _, nd := range nodes {
			nd.Close()
		}
		os.RemoveAll(dir)
	}
}
