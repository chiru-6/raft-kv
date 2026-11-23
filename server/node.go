package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/chiru-6/raft-kv/kv"
	"github.com/chiru-6/raft-kv/raft"
)

// NodeConfig describes one cluster member.
type NodeConfig struct {
	ID       string
	Peers    map[string]string // every member's ID -> base URL, including this one
	Listener net.Listener      // where to serve; if nil, Listen is used
	Listen   string            // e.g. ":8001"
	DataDir  string            // empty = in-memory (state lost on restart)
	Fsync    bool

	ElectionTimeout   time.Duration // minimum; default 300ms
	HeartbeatInterval time.Duration // default ElectionTimeout/6
	Verbose           bool
}

// Node is a running KV node: Raft + state machine + HTTP server.
type Node struct {
	Raft    *raft.Node
	Store   *kv.Store
	Addr    string
	http    *http.Server
	storage raft.Storage
}

// StartNode wires everything together and starts serving.
func StartNode(cfg NodeConfig) (*Node, error) {
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, fmt.Errorf("peer list must include this node (%s)", cfg.ID)
	}
	var storage raft.Storage = raft.NewMemoryStorage()
	if cfg.DataDir != "" {
		fs, err := raft.OpenFileStorage(cfg.DataDir, cfg.Fsync)
		if err != nil {
			return nil, err
		}
		storage = fs
	}

	var others []string
	for id := range cfg.Peers {
		if id != cfg.ID {
			others = append(others, id)
		}
	}
	rcfg := raft.Config{
		ID:                 cfg.ID,
		Peers:              others,
		Transport:          NewHTTPTransport(cfg.Peers),
		Storage:            storage,
		ElectionTimeoutMin: cfg.ElectionTimeout,
		HeartbeatInterval:  cfg.HeartbeatInterval,
	}
	if cfg.Verbose {
		rcfg.Logf = log.Printf
	}
	rn, applyCh, err := raft.NewNode(rcfg)
	if err != nil {
		storage.Close()
		return nil, err
	}
	store := kv.NewStore(rn, applyCh)

	ln := cfg.Listener
	if ln == nil {
		ln, err = net.Listen("tcp", cfg.Listen)
		if err != nil {
			rn.Stop()
			storage.Close()
			return nil, err
		}
	}
	srv := &Server{ID: cfg.ID, Peers: cfg.Peers, Store: store}
	hs := &http.Server{Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
	n := &Node{Raft: rn, Store: store, Addr: ln.Addr().String(), http: hs, storage: storage}
	go func() {
		if err := hs.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[%s] http: %v", cfg.ID, err)
		}
	}()
	return n, nil
}

// Close stops serving, stops Raft and closes storage.
func (n *Node) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Stop Raft first so in-flight client requests fail fast with 503.
	n.Raft.Stop()
	<-n.Store.Done()
	n.http.Shutdown(ctx)
	return n.storage.Close()
}
