// Package client is a Go client for the raft-kv HTTP API. It finds the
// leader, follows redirects, retries through elections and node failures,
// and tags writes with (client ID, sequence number) so a retried write is
// applied exactly once.
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chiru-6/raft-kv/server"
)

// Client talks to a cluster. It is safe for concurrent use; writes from one
// Client are serialized so their sequence numbers stay in order (use one
// Client per goroutine for parallel writes).
type Client struct {
	addrs []string
	http  *http.Client
	id    string

	mu     sync.Mutex
	leader string
	next   int

	writeMu sync.Mutex
	seq     uint64

	// Timeout bounds each operation including retries (default 10s).
	Timeout time.Duration
}

// New creates a client for the given node addresses ("host:port" or URLs).
func New(addrs []string) *Client {
	c := &Client{
		http: &http.Client{
			Timeout: 6 * time.Second,
			// Redirects are handled by hand so the client can remember the leader.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
			Transport:     &http.Transport{MaxIdleConnsPerHost: 32, IdleConnTimeout: 90 * time.Second},
		},
		id:      randomID(),
		Timeout: 10 * time.Second,
	}
	for _, a := range addrs {
		a = strings.TrimRight(strings.TrimSpace(a), "/")
		if a == "" {
			continue
		}
		if !strings.Contains(a, "://") {
			a = "http://" + a
		}
		c.addrs = append(c.addrs, a)
	}
	return c
}

func randomID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// Get performs a linearizable read.
func (c *Client) Get(ctx context.Context, key string) (value string, found bool, err error) {
	r, err := c.do(ctx, http.MethodGet, "/kv/"+escape(key), nil, false)
	if err != nil {
		return "", false, err
	}
	return r.Value, r.Found, nil
}

// StaleGet reads from any node without consensus; it may be out of date.
func (c *Client) StaleGet(ctx context.Context, key string) (value string, found bool, err error) {
	r, err := c.do(ctx, http.MethodGet, "/kv/"+escape(key)+"?stale=true", nil, false)
	if err != nil {
		return "", false, err
	}
	return r.Value, r.Found, nil
}

func (c *Client) Put(ctx context.Context, key, value string) error {
	_, err := c.do(ctx, http.MethodPut, "/kv/"+escape(key), []byte(value), true)
	return err
}

// Delete removes key and reports whether it existed.
func (c *Client) Delete(ctx context.Context, key string) (existed bool, err error) {
	r, err := c.do(ctx, http.MethodDelete, "/kv/"+escape(key), nil, true)
	if err != nil {
		return false, err
	}
	return r.Found, nil
}

// CAS sets key to value only if its current value is expected (or, with
// expectAbsent, only if it does not exist). It returns whether the swap
// happened and the value that was there before.
func (c *Client) CAS(ctx context.Context, key, expected string, expectAbsent bool, value string) (swapped bool, current string, found bool, err error) {
	body, _ := json.Marshal(server.CASRequest{Expected: expected, ExpectAbsent: expectAbsent, Value: value})
	r, err := c.do(ctx, http.MethodPost, "/cas/"+escape(key), body, true)
	if err != nil {
		return false, "", false, err
	}
	return r.OK, r.Value, r.Found, nil
}

// Status returns the raw /status JSON of one node.
func (c *Client) Status(ctx context.Context, addr string) (map[string]any, error) {
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(addr, "/")+"/status", nil)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out map[string]any
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

func escape(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

func (c *Client) target() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader != "" {
		return c.leader
	}
	a := c.addrs[c.next%len(c.addrs)]
	c.next++
	return a
}

func (c *Client) setLeader(addr string) {
	c.mu.Lock()
	c.leader = addr
	c.mu.Unlock()
}

// ErrTimeout is returned when no node could complete the request in time.
var ErrTimeout = errors.New("client: request timed out (no leader reachable)")

func (c *Client) do(ctx context.Context, method, path string, body []byte, write bool) (*server.Response, error) {
	if len(c.addrs) == 0 {
		return nil, errors.New("client: no addresses")
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}
	var seq uint64
	if write {
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		c.seq++
		seq = c.seq // reused on every retry of this operation
	}

	backoff := 20 * time.Millisecond
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return nil, fmt.Errorf("%w: last error: %v", ErrTimeout, lastErr)
			}
			return nil, ErrTimeout
		}
		base := c.target()
		req, err := http.NewRequestWithContext(ctx, method, base+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		if write {
			req.Header.Set("X-Client-ID", c.id)
			req.Header.Set("X-Seq", strconv.FormatUint(seq, 10))
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			c.setLeader("") // node down: try another
			sleep(ctx, backoff)
			continue
		}
		var r server.Response
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		_ = json.Unmarshal(raw, &r)

		switch {
		case resp.StatusCode == http.StatusOK:
			c.setLeader(base)
			return &r, nil
		case resp.StatusCode == http.StatusTemporaryRedirect && r.Leader != "":
			c.setLeader(r.Leader)
			continue
		case resp.StatusCode == http.StatusServiceUnavailable:
			lastErr = errors.New(r.Error)
			c.setLeader("")
			sleep(ctx, backoff)
			if backoff < 200*time.Millisecond {
				backoff *= 2
			}
			continue
		default:
			msg := r.Error
			if msg == "" {
				msg = strings.TrimSpace(string(raw))
			}
			return nil, fmt.Errorf("client: %s %s: %s: %s", method, path, resp.Status, msg)
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}
