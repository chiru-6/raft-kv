// Command kvctl is a command-line client for raft-kv.
//
//	kvctl put greeting hello
//	kvctl get greeting
//	kvctl cas counter 1 2
//	kvctl status
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/chiru-6/raft-kv/client"
)

const usage = `usage: kvctl [-cluster host:port,...] <command> [args]

commands:
  get <key>                    linearizable read
  get -stale <key>             read from any node (may be out of date)
  put <key> <value>
  del <key>
  cas <key> <expected> <new>   compare-and-swap
  cas -absent <key> <new>      set only if the key does not exist
  status                       show every node's Raft state
`

func main() {
	defaultCluster := os.Getenv("KV_CLUSTER")
	if defaultCluster == "" {
		defaultCluster = "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003"
	}
	cluster := flag.String("cluster", defaultCluster, "comma-separated node addresses (or $KV_CLUSTER)")
	timeout := flag.Duration("timeout", 10*time.Second, "overall timeout")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	addrs := strings.Split(*cluster, ",")
	c := client.New(addrs)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if err := run(ctx, c, addrs, args); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, c *client.Client, addrs, args []string) error {
	cmd, args := args[0], args[1:]
	need := func(n int) error {
		if len(args) != n {
			return fmt.Errorf("%s: expected %d argument(s)\n\n%s", cmd, n, usage)
		}
		return nil
	}
	switch cmd {
	case "get":
		stale := len(args) > 0 && args[0] == "-stale"
		if stale {
			args = args[1:]
		}
		if err := need(1); err != nil {
			return err
		}
		get := c.Get
		if stale {
			get = c.StaleGet
		}
		v, ok, err := get(ctx, args[0])
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("key %q not found", args[0])
		}
		fmt.Println(v)
	case "put", "set":
		if err := need(2); err != nil {
			return err
		}
		if err := c.Put(ctx, args[0], args[1]); err != nil {
			return err
		}
		fmt.Println("OK")
	case "del", "delete":
		if err := need(1); err != nil {
			return err
		}
		existed, err := c.Delete(ctx, args[0])
		if err != nil {
			return err
		}
		fmt.Println(map[bool]string{true: "deleted", false: "not found"}[existed])
	case "cas":
		absent := len(args) > 0 && args[0] == "-absent"
		var key, expected, value string
		if absent {
			args = args[1:]
			if err := need(2); err != nil {
				return err
			}
			key, value = args[0], args[1]
		} else {
			if err := need(3); err != nil {
				return err
			}
			key, expected, value = args[0], args[1], args[2]
		}
		ok, cur, found, err := c.CAS(ctx, key, expected, absent, value)
		if err != nil {
			return err
		}
		switch {
		case ok:
			fmt.Println("swapped")
		case found:
			fmt.Printf("not swapped (current value: %q)\n", cur)
		default:
			fmt.Println("not swapped (key does not exist)")
		}
	case "status":
		w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintln(w, "ADDRESS\tID\tSTATE\tTERM\tLEADER\tCOMMIT\tAPPLIED\tKEYS")
		for _, a := range addrs {
			st, err := c.Status(ctx, a)
			if err != nil {
				fmt.Fprintf(w, "%s\t-\tunreachable\t\t\t\t\t\n", a)
				continue
			}
			fmt.Fprintf(w, "%s\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n", a, st["id"], st["state"], st["term"],
				st["leader"], st["commitIndex"], st["lastApplied"], st["keys"])
		}
		w.Flush()
	default:
		return fmt.Errorf("unknown command %q\n\n%s", cmd, usage)
	}
	return nil
}
