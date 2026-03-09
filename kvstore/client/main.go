package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "kvstore/pb"
)

type routingClient struct {
	clients []pb.KvStoreClient
	conns   []*grpc.ClientConn
	n       int
}

var _ pb.KvStoreClient = (*routingClient)(nil)

func partitionFor(key string, n int) int { // hash partitioning and fanning out SCANs.
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(n)) //honestlly this makes things so easy
}

func (r *routingClient) Put(_ context.Context, in *pb.PutRequest, opts ...grpc.CallOption) (*pb.PutResponse, error) {
	c := r.clients[partitionFor(in.Key, r.n)]
	for {
		resp, err := c.Put(context.Background(), in, opts...)
		if err == nil {
			return resp, nil
		}
	}
}

func (r *routingClient) Swap(_ context.Context, in *pb.SwapRequest, opts ...grpc.CallOption) (*pb.SwapResponse, error) {
	c := r.clients[partitionFor(in.Key, r.n)]
	for {
		resp, err := c.Swap(context.Background(), in, opts...)
		if err == nil {
			return resp, nil
		}
	}
}

func (r *routingClient) Get(_ context.Context, in *pb.GetRequest, opts ...grpc.CallOption) (*pb.GetResponse, error) {
	c := r.clients[partitionFor(in.Key, r.n)]
	for {
		resp, err := c.Get(context.Background(), in, opts...)
		if err == nil {
			return resp, nil
		}
	}
}

func (r *routingClient) Delete(_ context.Context, in *pb.DeleteRequest, opts ...grpc.CallOption) (*pb.DeleteResponse, error) {
	c := r.clients[partitionFor(in.Key, r.n)]
	for {
		resp, err := c.Delete(context.Background(), in, opts...)
		if err == nil {
			return resp, nil
		}
	}
}

func (r *routingClient) Scan(_ context.Context, in *pb.ScanRequest, opts ...grpc.CallOption) (*pb.ScanResponse, error) {
	var allEntries []*pb.KeyValue
	for i := 0; i < r.n; i++ {
		for {
			resp, err := r.clients[i].Scan(context.Background(), in, opts...)
			if err == nil {
				allEntries = append(allEntries, resp.Entries...)
				break
			}
		}
	}
	sort.Slice(allEntries, func(i, j int) bool {
		return allEntries[i].Key < allEntries[j].Key
	})
	return &pb.ScanResponse{Entries: allEntries}, nil
}

// ScanPartial is like Scan but uses a per-server timeout, skipping down servers. ONLY USED FOR TEST6
func (r *routingClient) ScanPartial(in *pb.ScanRequest, timeout time.Duration) *pb.ScanResponse {
	var allEntries []*pb.KeyValue
	for i := 0; i < r.n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		resp, err := r.clients[i].Scan(ctx, in)
		cancel()
		if err == nil {
			allEntries = append(allEntries, resp.Entries...)
		}
	}
	sort.Slice(allEntries, func(i, j int) bool {
		return allEntries[i].Key < allEntries[j].Key
	})
	return &pb.ScanResponse{Entries: allEntries}
}

func (r *routingClient) Close() {
	for _, c := range r.conns {
		c.Close()
	}
}

func discoverAndConnect(managerAddr string) *routingClient {
	var servers []*pb.ServerInfo

	for {
		conn, err := grpc.NewClient(managerAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("failed to connect to manager: %v, retrying...", err)
			time.Sleep(time.Second)
			continue
		}

		client := pb.NewManagerClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := client.DiscoverServers(ctx, &pb.DiscoverServersRequest{})
		cancel()
		conn.Close()

		if err != nil {
			log.Printf("failed to discover servers: %v, retrying...", err)
			time.Sleep(time.Second)
			continue
		}

		servers = resp.Servers
		break
	}

	sort.Slice(servers, func(i, j int) bool {
		return servers[i].Id < servers[j].Id
	})

	n := len(servers)
	rc := &routingClient{
		clients: make([]pb.KvStoreClient, n),
		conns:   make([]*grpc.ClientConn, n),
		n:       n,
	}

	for i, s := range servers {
		conn, err := grpc.NewClient(s.Address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))
		if err != nil {
			log.Fatalf("failed to connect to server %d at %s: %v", s.Id, s.Address, err)
		}
		rc.conns[i] = conn
		rc.clients[i] = pb.NewKvStoreClient(conn)
	}

	log.Printf("discovered %d servers", n)
	return rc
}

func runStdin(client pb.KvStoreClient) {
	scanner := bufio.NewScanner(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}

		processCommand(client, writer, fields, line)
		writer.Flush()

		if fields[0] == "STOP" {
			return
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("error reading stdin: %v", err)
	}
}

func runTest1(client pb.KvStoreClient) {
	writer := bufio.NewWriter(os.Stdout)
	r := &testResult{}

	// PUT: insert new keys
	r.check(writer, "PUT aLOLOLOLOL hello", "PUT aLOLOLOLOL not_found", client)
	r.check(writer, "PUT bLOLZ world", "PUT bLOLZ not_found", client)
	r.check(writer, "PUT cLULULULULUL foo", "PUT cLULULULULUL not_found", client)
	// GET: existing key right away
	r.check(writer, "GET bLOLZ", "GET bLOLZ world", client)
	// PUT: overwrite existing key
	r.check(writer, "PUT aLOLOLOLOL updated", "PUT aLOLOLOLOL found", client)
	// GET: existing key after it was overwritten
	r.check(writer, "GET aLOLOLOLOL", "GET aLOLOLOLOL updated", client)
	// GET: non-existing key
	r.check(writer, "GET nonexistent", "GET nonexistent null", client)
	// SWAP: existing key
	r.check(writer, "SWAP bLOLZ swapped", "SWAP bLOLZ world", client)
	// SWAP: non-existing key
	r.check(writer, "SWAP missing val", "SWAP missing null", client)
	// GET: verify swap results
	r.check(writer, "GET bLOLZ", "GET bLOLZ swapped", client)
	r.check(writer, "GET missing", "GET missing val", client)
	// DELETE: existing key
	r.check(writer, "DELETE cLULULULULUL", "DELETE cLULULULULUL found", client)
	// DELETE: non-existing key
	r.check(writer, "DELETE cLULULULULUL", "DELETE cLULULULULUL not_found", client)
	// GET: verify delete
	r.check(writer, "GET cLULULULULUL", "GET cLULULULULUL null", client)
	// SCAN: range with multiple results
	r.check(writer, "SCAN aLOLOLOLOL missing", "SCAN aLOLOLOLOL missing BEGIN\n  aLOLOLOLOL updated\n  bLOLZ swapped\n  missing val\nSCAN END", client)
	// SCAN: empty range
	r.check(writer, "SCAN zzz zzzzz", "SCAN zzz zzzzz BEGIN\nSCAN END", client)
	// SCAN: single key range
	r.check(writer, "SCAN aLOLOLOLOL aLOLOLOLOL", "SCAN aLOLOLOLOL aLOLOLOLOL BEGIN\n  aLOLOLOLOL updated\nSCAN END", client)

	fmt.Fprintf(writer, "STOP\n")
	fmt.Fprintf(writer, "test1: %d passed, %d failed\n", r.passed, r.failed)
	writer.Flush()

	if r.failed > 0 {
		os.Exit(1)
	}
}

type testResult struct {
	passed int
	failed int
}

func (r *testResult) check(writer *bufio.Writer, cmd, expect string, client pb.KvStoreClient) {
	r.checkVerbose(writer, cmd, expect, client, true)
}

// I realized that printing out all the responses makes test3 unreadable
func (r *testResult) checkQuiet(writer *bufio.Writer, cmd, expect string, client pb.KvStoreClient) {
	r.checkVerbose(writer, cmd, expect, client, false)
}

func (r *testResult) checkVerbose(writer *bufio.Writer, cmd, expect string, client pb.KvStoreClient, verbose bool) {
	fields := strings.Fields(cmd)
	var buf strings.Builder
	bw := bufio.NewWriter(&buf)
	processCommand(client, bw, fields, cmd)
	bw.Flush()
	got := strings.TrimRight(buf.String(), "\n")

	if verbose {
		fmt.Fprintf(writer, "%s\n", got)
		writer.Flush()
	}

	if got != expect {
		fmt.Fprintf(writer, "  FAIL [%s]: expected %q, got %q\n", cmd, expect, got)
		writer.Flush()
		r.failed++
	} else {
		r.passed++
	}
}

func runTest2(client pb.KvStoreClient) {
	writer := bufio.NewWriter(os.Stdout)
	r := &testResult{}
	iterations := 20

	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("t2key%d", i)
		val1 := fmt.Sprintf("val%d_a", i)
		val2 := fmt.Sprintf("val%d_b", i)
		val3 := fmt.Sprintf("val%d_c", i)

		// GET on non-existing key
		r.check(writer, fmt.Sprintf("GET %s", key), fmt.Sprintf("GET %s null", key), client)

		// DELETE on non-existing key
		r.check(writer, fmt.Sprintf("DELETE %s", key), fmt.Sprintf("DELETE %s not_found", key), client)

		// PUT new key
		r.check(writer, fmt.Sprintf("PUT %s %s", key, val1), fmt.Sprintf("PUT %s not_found", key), client)

		// GET after PUT
		r.check(writer, fmt.Sprintf("GET %s", key), fmt.Sprintf("GET %s %s", key, val1), client)

		// PUT overwrite
		r.check(writer, fmt.Sprintf("PUT %s %s", key, val2), fmt.Sprintf("PUT %s found", key), client)

		// GET after overwrite
		r.check(writer, fmt.Sprintf("GET %s", key), fmt.Sprintf("GET %s %s", key, val2), client)

		// SWAP existing
		r.check(writer, fmt.Sprintf("SWAP %s %s", key, val3), fmt.Sprintf("SWAP %s %s", key, val2), client)

		// GET after SWAP
		r.check(writer, fmt.Sprintf("GET %s", key), fmt.Sprintf("GET %s %s", key, val3), client)

		// DELETE existing
		r.check(writer, fmt.Sprintf("DELETE %s", key), fmt.Sprintf("DELETE %s found", key), client)

		// GET after DELETE
		r.check(writer, fmt.Sprintf("GET %s", key), fmt.Sprintf("GET %s null", key), client)

		// SWAP on non-existing (creates key)
		r.check(writer, fmt.Sprintf("SWAP %s %s", key, val1), fmt.Sprintf("SWAP %s null", key), client)

		// GET after SWAP-create
		r.check(writer, fmt.Sprintf("GET %s", key), fmt.Sprintf("GET %s %s", key, val1), client)
	}

	// SCAN: all keys should exist
	var scanEntries []string
	keys := make([]string, iterations)
	for i := 0; i < iterations; i++ {
		keys[i] = fmt.Sprintf("t2key%d", i)
	}
	// expected key ordering (lexicographically) instead of 0-19 numerically
	sortedKeys := make([]string, len(keys))
	copy(sortedKeys, keys)
	for i := 0; i < len(sortedKeys); i++ {
		for j := i + 1; j < len(sortedKeys); j++ {
			if sortedKeys[j] < sortedKeys[i] {
				sortedKeys[i], sortedKeys[j] = sortedKeys[j], sortedKeys[i]
			}
		}
	}
	for _, k := range sortedKeys {
		idx := 0
		for idx < iterations {
			if keys[idx] == k {
				break
			}
			idx++
		}
		scanEntries = append(scanEntries, fmt.Sprintf("  %s val%d_a", k, idx))
	}
	scanExpect := fmt.Sprintf("SCAN t2key0 t2key99 BEGIN\n%s\nSCAN END", strings.Join(scanEntries, "\n"))
	r.check(writer, "SCAN t2key0 t2key99", scanExpect, client)

	// SCAN: partial range
	r.check(writer, "SCAN t2key0 t2key0", "SCAN t2key0 t2key0 BEGIN\n  t2key0 val0_a\nSCAN END", client)

	// SCAN: empty range
	r.check(writer, "SCAN zzz999 zzz999", "SCAN zzz999 zzz999 BEGIN\nSCAN END", client)

	// Cleanup: delete all keys
	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("t2key%d", i)
		r.check(writer, fmt.Sprintf("DELETE %s", key), fmt.Sprintf("DELETE %s found", key), client)
	}

	// Verify all deleted
	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("t2key%d", i)
		r.check(writer, fmt.Sprintf("GET %s", key), fmt.Sprintf("GET %s null", key), client)
	}

	fmt.Fprintf(writer, "STOP\n")
	fmt.Fprintf(writer, "test2: %d passed, %d failed\n", r.passed, r.failed)
	writer.Flush()

	if r.failed > 0 {
		os.Exit(1)
	}
}

func runTest3(client pb.KvStoreClient, clientID int) {
	writer := bufio.NewWriter(os.Stdout)
	r := &testResult{}
	iterations := 10
	prefix := fmt.Sprintf("c%d", clientID)

	for round := 0; round < 300; round++ {
		// Each round: PUT all keys, GET all, SWAP all, SCAN, DELETE all, verify deleted
		for i := 0; i < iterations; i++ {
			key := fmt.Sprintf("%s_r%d_k%d", prefix, round, i)
			val := fmt.Sprintf("%s_r%d_v%d", prefix, round, i)
			r.checkQuiet(writer, fmt.Sprintf("PUT %s %s", key, val), fmt.Sprintf("PUT %s not_found", key), client)
		}

		// GET all keys back
		for i := 0; i < iterations; i++ {
			key := fmt.Sprintf("%s_r%d_k%d", prefix, round, i)
			val := fmt.Sprintf("%s_r%d_v%d", prefix, round, i)
			r.checkQuiet(writer, fmt.Sprintf("GET %s %s", key, val), fmt.Sprintf("GET %s %s", key, val), client)
		}

		// PUT overwrite all
		for i := 0; i < iterations; i++ {
			key := fmt.Sprintf("%s_r%d_k%d", prefix, round, i)
			val2 := fmt.Sprintf("%s_r%d_w%d", prefix, round, i)
			r.checkQuiet(writer, fmt.Sprintf("PUT %s %s", key, val2), fmt.Sprintf("PUT %s found", key), client)
		}

		// SWAP all back, verify old values
		for i := 0; i < iterations; i++ {
			key := fmt.Sprintf("%s_r%d_k%d", prefix, round, i)
			val2 := fmt.Sprintf("%s_r%d_w%d", prefix, round, i)
			val3 := fmt.Sprintf("%s_r%d_x%d", prefix, round, i)
			r.checkQuiet(writer, fmt.Sprintf("SWAP %s %s", key, val3), fmt.Sprintf("SWAP %s %s", key, val2), client)
		}

		// GET after SWAP
		for i := 0; i < iterations; i++ {
			key := fmt.Sprintf("%s_r%d_k%d", prefix, round, i)
			val3 := fmt.Sprintf("%s_r%d_x%d", prefix, round, i)
			r.checkQuiet(writer, fmt.Sprintf("GET %s", key), fmt.Sprintf("GET %s %s", key, val3), client)
		}

		// SCAN this round's keys
		startKey := fmt.Sprintf("%s_r%d_k0", prefix, round)
		endKey := fmt.Sprintf("%s_r%d_k999", prefix, round)
		// Build expected scan output
		var scanLines []string
		for i := 0; i < iterations; i++ {
			key := fmt.Sprintf("%s_r%d_k%d", prefix, round, i)
			val3 := fmt.Sprintf("%s_r%d_x%d", prefix, round, i)
			scanLines = append(scanLines, fmt.Sprintf("  %s %s", key, val3))
		}
		// Keys are already in lex order
		scanExpect := fmt.Sprintf("SCAN %s %s BEGIN\n%s\nSCAN END", startKey, endKey, strings.Join(scanLines, "\n"))
		r.checkQuiet(writer, fmt.Sprintf("SCAN %s %s", startKey, endKey), scanExpect, client)

		// DELETE all
		for i := 0; i < iterations; i++ {
			key := fmt.Sprintf("%s_r%d_k%d", prefix, round, i)
			r.checkQuiet(writer, fmt.Sprintf("DELETE %s", key), fmt.Sprintf("DELETE %s found", key), client)
		}

		// Verify all deleted
		for i := 0; i < iterations; i++ {
			key := fmt.Sprintf("%s_r%d_k%d", prefix, round, i)
			r.checkQuiet(writer, fmt.Sprintf("GET %s", key), fmt.Sprintf("GET %s null", key), client)
		}

		// SCAN empty after delete
		r.checkQuiet(writer, fmt.Sprintf("SCAN %s %s", startKey, endKey), fmt.Sprintf("SCAN %s %s BEGIN\nSCAN END", startKey, endKey), client)
	}

	fmt.Fprintf(writer, "STOP\n")
	fmt.Fprintf(writer, "test3 (client %d): %d passed, %d failed\n", clientID, r.passed, r.failed)
	writer.Flush()

	if r.failed > 0 {
		os.Exit(1)
	}
}

// Test 4: Concurrent relay on a shared key.
// 3 clients run simultaneously. Client 0 initializes key with "c0".
// Client 1 polls until it sees "c0", swaps to "c1".
// Client 2 polls until it sees "c1", swaps to "c2".
// Client 0 polls until it sees "c2", swaps to "c0".
// This relay repeats many times. Each client must observe all 3 values.
func runTest4(client pb.KvStoreClient, clientID int) {
	writer := bufio.NewWriter(os.Stdout)
	ctx := context.Background()
	passed := 0
	failed := 0
	rounds := 50
	key := "t4relay"
	myVal := fmt.Sprintf("c%d", clientID)
	waitFor := fmt.Sprintf("c%d", (clientID+2)%3) // c0 waits for c2, c1 waits for c0, c2 waits for c1
	seen := map[string]bool{}

	fail := func(msg string) {
		fmt.Fprintf(writer, "  FAIL: %s\n", msg)
		writer.Flush()
		failed++
	}

	// Client 0 initializes the key
	if clientID == 0 {
		_, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: myVal})
		if err != nil {
			log.Fatalf("PUT failed: %v", err)
		}
		seen[myVal] = true
		passed++
	}

	for round := 0; round < rounds; round++ {
		// Poll GET until we see our predecessor's value
		for {
			resp, err := client.Get(ctx, &pb.GetRequest{Key: key})
			if err != nil {
				log.Fatalf("GET failed: %v", err)
			}
			if resp.Found {
				seen[resp.Value] = true
				if resp.Value == waitFor {
					break
				}
			}
			time.Sleep(time.Millisecond)
		}

		// SWAP to our value
		swapResp, err := client.Swap(ctx, &pb.SwapRequest{Key: key, Value: myVal})
		if err != nil {
			log.Fatalf("SWAP failed: %v", err)
		}
		seen[swapResp.OldValue] = true
		if swapResp.OldValue != waitFor {
			fail(fmt.Sprintf("client %d round %d: SWAP expected old=%s, got old=%s", clientID, round, waitFor, swapResp.OldValue))
		} else {
			passed++
		}
	}

	// Verify all 3 client values were seen
	for i := 0; i < 3; i++ {
		v := fmt.Sprintf("c%d", i)
		if !seen[v] {
			fail(fmt.Sprintf("client %d never saw value %s", clientID, v))
		} else {
			passed++
		}
	}

	fmt.Fprintf(writer, "STOP\n")
	fmt.Fprintf(writer, "test4 (client %d): %d passed, %d failed\n", clientID, passed, failed)
	writer.Flush()

	if failed > 0 {
		os.Exit(1)
	}
}

// Test 5: Concurrent contention on multiple shared keys.
// 3 clients run simultaneously. Each round, each client randomly picks a key,
// randomly PUTs or SWAPs it to its own tagged value, then GETs all keys.
// At the end, every client must have seen every key holding every client's value
// at least once.
func runTest5(client pb.KvStoreClient, clientID int) {
	writer := bufio.NewWriter(os.Stdout)
	ctx := context.Background()
	passed := 0
	failed := 0
	numClients := 3
	numKeys := 5
	rounds := 500
	myTag := fmt.Sprintf("c%d", clientID)

	// seen[key_index][client_index] = true if we've seen that client's value on that key
	seen := make([][]bool, numKeys)
	for i := range seen {
		seen[i] = make([]bool, numClients)
	}

	fail := func(msg string) {
		fmt.Fprintf(writer, "  FAIL: %s\n", msg)
		writer.Flush()
		failed++
	}

	// Simple deterministic "random" per client to avoid import
	seed := uint32(clientID*7 + 13)
	nextRand := func(n int) int {
		seed = seed*1103515245 + 12345
		return int((seed >> 16) % uint32(n))
	}

	// Client 0 initializes all keys; others wait until all keys exist
	if clientID == 0 {
		for k := 0; k < numKeys; k++ {
			key := fmt.Sprintf("t5key%d", k)
			_, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: myTag})
			if err != nil {
				log.Fatalf("PUT failed: %v", err)
			}
			seen[k][clientID] = true
			passed++
		}
		// Signal other clients by writing a ready key per client
		for c := 1; c < numClients; c++ {
			_, err := client.Put(ctx, &pb.PutRequest{Key: fmt.Sprintf("t5ready%d", c), Value: "go"})
			if err != nil {
				log.Fatalf("PUT ready failed: %v", err)
			}
		}
	} else {
		// Wait for client 0 to signal us
		for {
			resp, err := client.Get(ctx, &pb.GetRequest{Key: fmt.Sprintf("t5ready%d", clientID)})
			if err != nil {
				log.Fatalf("GET ready failed: %v", err)
			}
			if resp.Found {
				break
			}
			time.Sleep(time.Millisecond)
		}
	}

	// Synchronization: all clients write a "started" key, then wait for all to appear
	_, _ = client.Put(ctx, &pb.PutRequest{Key: fmt.Sprintf("t5started%d", clientID), Value: "1"})
	for c := 0; c < numClients; c++ {
		for {
			resp, err := client.Get(ctx, &pb.GetRequest{Key: fmt.Sprintf("t5started%d", c)})
			if err != nil {
				log.Fatalf("GET started failed: %v", err)
			}
			if resp.Found {
				break
			}
			time.Sleep(time.Millisecond)
		}
	}

	for round := 0; round < rounds; round++ {
		// Pick a random key and randomly PUT or SWAP our value
		k := nextRand(numKeys)
		key := fmt.Sprintf("t5key%d", k)
		val := fmt.Sprintf("%s_r%d", myTag, round)

		if nextRand(2) == 0 {
			// PUT
			_, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: val})
			if err != nil {
				log.Fatalf("PUT failed: %v", err)
			}
			passed++
		} else {
			// SWAP
			swapResp, err := client.Swap(ctx, &pb.SwapRequest{Key: key, Value: val})
			if err != nil {
				log.Fatalf("SWAP failed: %v", err)
			}
			if swapResp.Found {
				// Record which client's value we saw
				for c := 0; c < numClients; c++ {
					if strings.HasPrefix(swapResp.OldValue, fmt.Sprintf("c%d", c)) {
						seen[k][c] = true
					}
				}
			}
			passed++
		}

		// GET all keys and record which client values we observe
		for ki := 0; ki < numKeys; ki++ {
			getKey := fmt.Sprintf("t5key%d", ki)
			resp, err := client.Get(ctx, &pb.GetRequest{Key: getKey})
			if err != nil {
				log.Fatalf("GET failed: %v", err)
			}
			if resp.Found {
				for c := 0; c < numClients; c++ {
					if strings.HasPrefix(resp.Value, fmt.Sprintf("c%d", c)) {
						seen[ki][c] = true
					}
				}
				passed++
			} else {
				fail(fmt.Sprintf("GET %s returned null (round %d)", getKey, round))
			}
		}
	}

	// Verify: every key must have been seen with every client's value
	allSeen := true
	for k := 0; k < numKeys; k++ {
		for c := 0; c < numClients; c++ {
			if !seen[k][c] {
				fail(fmt.Sprintf("client %d never saw t5key%d with c%d's value", clientID, k, c))
				allSeen = false
			}
		}
	}
	if allSeen {
		passed++
	}

	fmt.Fprintf(writer, "STOP\n")
	fmt.Fprintf(writer, "test5 (client %d): %d passed, %d failed\n", clientID, passed, failed)
	writer.Flush()

	if failed > 0 {
		os.Exit(1)
	}
}

// Test 6
func runTest6(client pb.KvStoreClient) {
	writer := bufio.NewWriter(os.Stdout)
	reader := bufio.NewReader(os.Stdin)
	ctx := context.Background()
	passed := 0
	failed := 0

	fail := func(msg string) {
		fmt.Fprintf(writer, "  FAIL: %s\n", msg)
		writer.Flush()
		failed++
	}

	waitEnter := func(msg string) {
		fmt.Fprintf(writer, "\n>>> %s\n>>> Press Enter to continue...\n", msg)
		writer.Flush()
		reader.ReadString('\n')
	}

	// Pre-computed partition assignments for n=3 (FNV-1a):
	keys := map[int][]string{
		0: {"bravo", "foxtrot", "hotel", "india", "lima", "november", "papa", "uniform", "victor"},
		1: {"charlie", "delta", "aaa", "juliet", "romeo", "tango", "xray", "banana", "cherry"},
		2: {"alpha", "echo", "golf", "kilo", "mike", "oscar", "quebec", "sierra", "whiskey"},
	}
	killPartition := 1

	waitEnter("Step 1: Ensure cluster with 3 servers is running")

	fmt.Fprintf(writer, "\n=== Step 2: PUT and SWAP across all partitions ===\n")
	writer.Flush()
	for p := 0; p < 3; p++ {
		for _, key := range keys[p] {
			val := "v_" + key
			_, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: val})
			if err != nil {
				fail(fmt.Sprintf("PUT %s: %v", key, err))
			} else {
				fmt.Fprintf(writer, "  PUT %s=%s (partition %d) OK\n", key, val, p)
				passed++
			}
		}
	}
	for p := 0; p < 3; p++ {
		key := keys[p][0]
		newVal := "sw_" + key
		resp, err := client.Swap(ctx, &pb.SwapRequest{Key: key, Value: newVal})
		if err != nil {
			fail(fmt.Sprintf("SWAP %s: %v", key, err))
		} else {
			fmt.Fprintf(writer, "  SWAP %s old=%s new=%s (partition %d) OK\n", key, resp.OldValue, newVal, p)
			passed++
		}
	}
	writer.Flush()

	fmt.Fprintf(writer, "\n=== Step 3: GET and SCAN verify all data ===\n")
	writer.Flush()
	for p := 0; p < 3; p++ {
		for _, key := range keys[p] {
			resp, err := client.Get(ctx, &pb.GetRequest{Key: key})
			if err != nil {
				fail(fmt.Sprintf("GET %s: %v", key, err))
			} else if !resp.Found {
				fail(fmt.Sprintf("GET %s: not found", key))
			} else {
				fmt.Fprintf(writer, "  GET %s=%s (partition %d) OK\n", key, resp.Value, p)
				passed++
			}
		}
	}
	scanResp, err := client.Scan(ctx, &pb.ScanRequest{KeyStart: "a", KeyEnd: "z"})
	if err != nil {
		fail(fmt.Sprintf("SCAN a-z: %v", err))
	} else {
		fmt.Fprintf(writer, "  SCAN a-z returned %d entries OK\n", len(scanResp.Entries))
		passed++
	}
	writer.Flush()

	waitEnter(fmt.Sprintf("Step 4: Kill server %d now (port %d)", killPartition, 3777+killPartition))

	fmt.Fprintf(writer, "\n=== Step 5: GET and SCAN on unaffected partitions ===\n")
	writer.Flush()
	for p := 0; p < 3; p++ {
		if p == killPartition {
			continue
		}
		key := keys[p][0]
		resp, err := client.Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			fail(fmt.Sprintf("GET %s (partition %d): %v", key, p, err))
		} else if !resp.Found {
			fail(fmt.Sprintf("GET %s (partition %d): not found", key, p))
		} else {
			fmt.Fprintf(writer, "  GET %s=%s (partition %d) OK\n", key, resp.Value, p)
			passed++
		}
	}
	// ScanPartial with 5s per-server timeout: returns partial results, skipping the down server
	rc := client.(*routingClient)
	scanResp2 := rc.ScanPartial(&pb.ScanRequest{KeyStart: "a", KeyEnd: "z"}, 5*time.Second)
	fmt.Fprintf(writer, "  SCAN a-z returned %d entries (missing partition %d keys) OK\n", len(scanResp2.Entries), killPartition)
	passed++
	writer.Flush()

	fmt.Fprintf(writer, "\n=== Step 6: GET on failed partition %d (expecting timeout) ===\n", killPartition)
	writer.Flush()
	failedKey := keys[killPartition][0]
	done := make(chan *pb.GetResponse, 1)
	go func() {
		resp, _ := client.Get(ctx, &pb.GetRequest{Key: failedKey})
		done <- resp
	}()
	select {
	case <-done:
		fail(fmt.Sprintf("GET %s should have blocked but returned", failedKey))
	case <-time.After(5 * time.Second):
		fmt.Fprintf(writer, "  GET %s blocked as expected (partition %d is down)\n", failedKey, killPartition)
		passed++
	}
	writer.Flush()

	waitEnter(fmt.Sprintf("Step 7: Restart server %d now (port %d, same backer path)", killPartition, 3777+killPartition))

	select {
	case <-done:
		fmt.Fprintf(writer, "  Background GET unblocked after restart\n")
	case <-time.After(30 * time.Second):
		fail("background GET still blocked after restart")
	}
	writer.Flush()

	fmt.Fprintf(writer, "\n=== Step 8: GET on recovered partition ===\n")
	writer.Flush()
	resp, err := client.Get(ctx, &pb.GetRequest{Key: failedKey})
	if err != nil {
		fail(fmt.Sprintf("GET %s: %v", failedKey, err))
	} else if !resp.Found {
		fail(fmt.Sprintf("GET %s: not found after recovery", failedKey))
	} else {
		expected := "sw_" + failedKey
		if resp.Value == expected {
			fmt.Fprintf(writer, "  GET %s=%s recovered correctly!\n", failedKey, resp.Value)
			passed++
		} else {
			fail(fmt.Sprintf("GET %s: expected %s, got %s", failedKey, expected, resp.Value))
		}
	}

	fmt.Fprintf(writer, "\nSTOP\n")
	fmt.Fprintf(writer, "test6: %d passed, %d failed\n", passed, failed)
	writer.Flush()

	if failed > 0 {
		os.Exit(1)
	}
}

func processCommand(client pb.KvStoreClient, writer *bufio.Writer, fields []string, line string) {
	ctx := context.Background()

	switch fields[0] {
	case "PUT":
		if len(fields) < 3 {
			log.Printf("invalid PUT command: %s", line)
			return
		}
		resp, err := client.Put(ctx, &pb.PutRequest{Key: fields[1], Value: fields[2]})
		if err != nil {
			log.Fatalf("PUT failed: %v", err)
		}
		if resp.Found {
			fmt.Fprintf(writer, "PUT %s found\n", fields[1])
		} else {
			fmt.Fprintf(writer, "PUT %s not_found\n", fields[1])
		}

	case "SWAP":
		if len(fields) < 3 {
			log.Printf("invalid SWAP command: %s", line)
			return
		}
		resp, err := client.Swap(ctx, &pb.SwapRequest{Key: fields[1], Value: fields[2]})
		if err != nil {
			log.Fatalf("SWAP failed: %v", err)
		}
		if resp.Found {
			fmt.Fprintf(writer, "SWAP %s %s\n", fields[1], resp.OldValue)
		} else {
			fmt.Fprintf(writer, "SWAP %s null\n", fields[1])
		}

	case "GET":
		if len(fields) < 2 {
			log.Printf("invalid GET command: %s", line)
			return
		}
		resp, err := client.Get(ctx, &pb.GetRequest{Key: fields[1]})
		if err != nil {
			log.Fatalf("GET failed: %v", err)
		}
		if resp.Found {
			fmt.Fprintf(writer, "GET %s %s\n", fields[1], resp.Value)
		} else {
			fmt.Fprintf(writer, "GET %s null\n", fields[1])
		}

	case "DELETE":
		if len(fields) < 2 {
			log.Printf("invalid DELETE command: %s", line)
			return
		}
		resp, err := client.Delete(ctx, &pb.DeleteRequest{Key: fields[1]})
		if err != nil {
			log.Fatalf("DELETE failed: %v", err)
		}
		if resp.Found {
			fmt.Fprintf(writer, "DELETE %s found\n", fields[1])
		} else {
			fmt.Fprintf(writer, "DELETE %s not_found\n", fields[1])
		}

	case "SCAN":
		if len(fields) < 3 {
			log.Printf("invalid SCAN command: %s", line)
			return
		}
		resp, err := client.Scan(ctx, &pb.ScanRequest{KeyStart: fields[1], KeyEnd: fields[2]})
		if err != nil {
			log.Fatalf("SCAN failed: %v", err)
		}
		fmt.Fprintf(writer, "SCAN %s %s BEGIN\n", fields[1], fields[2])
		for _, entry := range resp.Entries {
			fmt.Fprintf(writer, "  %s %s\n", entry.Key, entry.Value)
		}
		fmt.Fprintf(writer, "SCAN END\n")

	case "STOP":
		fmt.Fprintf(writer, "STOP\n")

	default:
		log.Printf("unknown command: %s", fields[0])
	}
}

func main() {
	server := flag.String("server", "", "server address (single-server mode)")
	manager := flag.String("manager", "", "manager address for server discovery")
	test := flag.Int("test", 0, "run built-in test (1-5), 0 for stdin mode")
	clientID := flag.Int("clientid", 0, "client ID for multi-client tests")
	flag.Parse()

	var client pb.KvStoreClient
	var cleanup func()

	if *manager != "" {
		rc := discoverAndConnect(*manager)
		client = rc
		cleanup = rc.Close
	} else if *server != "" {
		conn, err := grpc.NewClient(*server, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("failed to connect: %v", err)
		}
		client = pb.NewKvStoreClient(conn)
		cleanup = func() { conn.Close() }
	} else {
		log.Fatal("must specify --server or --manager")
	}
	defer cleanup()

	switch *test {
	case 0:
		runStdin(client)
	case 1:
		runTest1(client)
	case 2:
		runTest2(client)
	case 3:
		runTest3(client, *clientID)
	case 4:
		runTest4(client, *clientID)
	case 5:
		runTest5(client, *clientID)
	case 6:
		runTest6(client)
	default:
		log.Fatalf("unknown test: %d", *test)
	}
}
