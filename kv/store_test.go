package kv

import (
	"context"
	"testing"
	"time"

	"github.com/chiru-6/raft-kv/raft"
)

func newSolo(t *testing.T) *Store {
	t.Helper()
	n, ch, err := raft.NewNode(raft.Config{ID: "solo", ElectionTimeoutMin: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Stop)
	s := NewStore(n, ch)
	for n.Status().State != "leader" {
		time.Sleep(5 * time.Millisecond)
	}
	return s
}

func TestStoreOperations(t *testing.T) {
	s := newSolo(t)
	ctx := context.Background()
	exec := func(c Command) Result {
		t.Helper()
		r, err := s.Execute(ctx, c)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	exec(Command{Op: OpPut, Key: "a", Value: "1"})
	if r := exec(Command{Op: OpGet, Key: "a"}); !r.Found || r.Value != "1" {
		t.Fatalf("get a = %+v", r)
	}
	if r := exec(Command{Op: OpCAS, Key: "a", Expected: "2", Value: "3"}); r.OK {
		t.Fatal("CAS with wrong expected value succeeded")
	}
	if r := exec(Command{Op: OpCAS, Key: "a", Expected: "1", Value: "3"}); !r.OK {
		t.Fatal("CAS with right expected value failed")
	}
	if r := exec(Command{Op: OpDelete, Key: "a"}); !r.Found {
		t.Fatal("delete should report the key existed")
	}
	if r := exec(Command{Op: OpGet, Key: "a"}); r.Found {
		t.Fatal("key survived delete")
	}
	if _, err := s.Execute(ctx, Command{Op: "explode", Key: "a"}); err == nil {
		t.Fatal("unknown op was accepted")
	}
}

func TestDuplicateWritesAppliedOnce(t *testing.T) {
	s := newSolo(t)
	ctx := context.Background()
	put := Command{Op: OpPut, Key: "k", Value: "first", ClientID: "c1", Seq: 1}
	s.Execute(ctx, put)
	s.Execute(ctx, Command{Op: OpPut, Key: "k", Value: "second", ClientID: "c2", Seq: 1})
	s.Execute(ctx, put) // retry of c1's seq 1 must not overwrite "second"
	if v, _ := s.StaleGet("k"); v != "second" {
		t.Fatalf("k = %q, want %q", v, "second")
	}
}
