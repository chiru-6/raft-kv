// Package server exposes a kv.Store over HTTP and implements the Raft
// transport as JSON over HTTP.
//
// Client API (all responses are JSON):
//
//	GET    /kv/{key}            linearizable read (add ?stale=true for a fast local read)
//	PUT    /kv/{key}            body is the value
//	DELETE /kv/{key}
//	POST   /cas/{key}           {"expected": "...", "expectAbsent": false, "value": "..."}
//	GET    /status              Raft state of this node
//
// Optional headers X-Client-ID and X-Seq make writes idempotent across retries.
// A follower answers 307 with the leader's address in Location and in the body.
//
// Raft RPCs: POST /raft/vote and POST /raft/append.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/chiru-6/raft-kv/kv"
	"github.com/chiru-6/raft-kv/raft"
)

// MaxValueSize bounds PUT bodies.
const MaxValueSize = 1 << 20

// Server serves one node.
type Server struct {
	ID      string
	Peers   map[string]string // node ID -> base URL ("http://host:port"), including this node
	Store   *kv.Store
	Timeout time.Duration // per client request; default 5s
}

// Response is the body of every client API reply.
type Response struct {
	Key    string `json:"key,omitempty"`
	Value  string `json:"value,omitempty"`
	Found  bool   `json:"found"`
	OK     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
	Leader string `json:"leader,omitempty"` // leader's base URL, on redirects
}

// CASRequest is the body of POST /cas/{key}.
type CASRequest struct {
	Expected     string `json:"expected"`
	ExpectAbsent bool   `json:"expectAbsent"`
	Value        string `json:"value"`
}

// Handler returns the HTTP handler for both the client API and Raft RPCs.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/vote", s.handleVote)
	mux.HandleFunc("POST /raft/append", s.handleAppend)
	mux.HandleFunc("GET /kv/{key...}", s.handleGet)
	mux.HandleFunc("PUT /kv/{key...}", s.handlePut)
	mux.HandleFunc("DELETE /kv/{key...}", s.handleDelete)
	mux.HandleFunc("POST /cas/{key...}", s.handleCAS)
	mux.HandleFunc("GET /status", s.handleStatus)
	return mux
}

func (s *Server) handleVote(w http.ResponseWriter, r *http.Request) {
	var req raft.RequestVoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, s.Store.Node().HandleRequestVote(&req))
}

func (s *Server) handleAppend(w http.ResponseWriter, r *http.Request) {
	var req raft.AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, s.Store.Node().HandleAppendEntries(&req))
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st := s.Store.Node().Status()
	writeJSON(w, http.StatusOK, struct {
		raft.Status
		Keys      int    `json:"keys"`
		LeaderURL string `json:"leaderUrl,omitempty"`
	}{st, s.Store.Len(), s.Peers[st.Leader]})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if stale, _ := strconv.ParseBool(r.URL.Query().Get("stale")); stale {
		v, ok := s.Store.StaleGet(key)
		writeJSON(w, http.StatusOK, Response{Key: key, Value: v, Found: ok, OK: true})
		return
	}
	s.execute(w, r, kv.Command{Op: kv.OpGet, Key: key})
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxValueSize))
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, Response{Error: err.Error()})
		return
	}
	s.execute(w, r, kv.Command{Op: kv.OpPut, Key: r.PathValue("key"), Value: string(body)})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	s.execute(w, r, kv.Command{Op: kv.OpDelete, Key: r.PathValue("key")})
}

func (s *Server) handleCAS(w http.ResponseWriter, r *http.Request) {
	var req CASRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2*MaxValueSize)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, Response{Error: err.Error()})
		return
	}
	s.execute(w, r, kv.Command{
		Op: kv.OpCAS, Key: r.PathValue("key"),
		Expected: req.Expected, ExpectAbsent: req.ExpectAbsent, Value: req.Value,
	})
}

func (s *Server) execute(w http.ResponseWriter, r *http.Request, cmd kv.Command) {
	if cmd.Key == "" {
		writeJSON(w, http.StatusBadRequest, Response{Error: "empty key"})
		return
	}
	cmd.ClientID = r.Header.Get("X-Client-ID")
	if cmd.ClientID != "" {
		seq, err := strconv.ParseUint(r.Header.Get("X-Seq"), 10, 64)
		if err != nil || seq == 0 {
			writeJSON(w, http.StatusBadRequest, Response{Error: "X-Seq must be a positive integer when X-Client-ID is set"})
			return
		}
		cmd.Seq = seq
	}

	timeout := s.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	res, err := s.Store.Execute(ctx, cmd)
	var nl *kv.NotLeaderError
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, Response{Key: cmd.Key, Value: res.Value, Found: res.Found, OK: res.OK})
	case errors.As(err, &nl):
		leaderURL := s.Peers[nl.Leader]
		if leaderURL == "" {
			writeJSON(w, http.StatusServiceUnavailable, Response{Error: "no leader elected yet; retry"})
			return
		}
		w.Header().Set("Location", leaderURL+r.URL.RequestURI())
		writeJSON(w, http.StatusTemporaryRedirect, Response{Error: "not leader", Leader: leaderURL})
	default:
		// Leadership lost, timeout, or shutdown: the outcome is unknown, so
		// the client should retry (with the same X-Seq to stay idempotent).
		writeJSON(w, http.StatusServiceUnavailable, Response{Error: err.Error()})
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// ---------------------------------------------------------------------------
// HTTP transport for Raft

// HTTPTransport sends Raft RPCs as JSON over HTTP with keep-alive connections.
type HTTPTransport struct {
	Peers  map[string]string // node ID -> base URL
	Client *http.Client
}

func NewHTTPTransport(peers map[string]string) *HTTPTransport {
	return &HTTPTransport{
		Peers: peers,
		Client: &http.Client{Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
		}},
	}
}

func (t *HTTPTransport) call(ctx context.Context, peer, path string, req, resp any) error {
	base, ok := t.Peers[peer]
	if !ok {
		return fmt.Errorf("unknown peer %q", peer)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	hreq.Header.Set("Content-Type", "application/json")
	hresp, err := t.Client.Do(hreq)
	if err != nil {
		return err
	}
	defer hresp.Body.Close()
	if hresp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, hresp.Body)
		return fmt.Errorf("%s%s: %s", base, path, hresp.Status)
	}
	return json.NewDecoder(hresp.Body).Decode(resp)
}

func (t *HTTPTransport) RequestVote(ctx context.Context, peer string, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	var resp raft.RequestVoteResponse
	return &resp, t.call(ctx, peer, "/raft/vote", req, &resp)
}

func (t *HTTPTransport) AppendEntries(ctx context.Context, peer string, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	var resp raft.AppendEntriesResponse
	return &resp, t.call(ctx, peer, "/raft/append", req, &resp)
}
