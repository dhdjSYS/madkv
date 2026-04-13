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
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "kvstore/pb"
)

const notLeaderPrefix = "NOT_LEADER:"

const rpcTimeout = 5 * time.Second

type partitionGroup struct {
	mu      sync.Mutex
	addrs   []string
	conns   []*grpc.ClientConn
	clients []pb.KvStoreClient
	leader  int
	trial   int
}

func (pg *partitionGroup) pickLocked() (int, pb.KvStoreClient) {
	idx := pg.leader
	if idx < 0 {
		idx = pg.trial % len(pg.clients)
	}
	return idx, pg.clients[idx]
}

func (pg *partitionGroup) do(fn func(context.Context, pb.KvStoreClient) error) error {
	for {
		pg.mu.Lock()
		idx, c := pg.pickLocked()
		pg.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		err := fn(ctx, c)
		cancel()
		if err == nil {
			pg.mu.Lock()
			pg.leader = idx
			pg.mu.Unlock()
			return nil
		}

		if hint, ok := parseNotLeader(err); ok {
			pg.mu.Lock()
			if hint >= 0 && int(hint) < len(pg.clients) {
				pg.leader = int(hint)
			} else {
				pg.leader = -1
				pg.trial++
			}
			pg.mu.Unlock()
			continue
		}

		pg.mu.Lock()
		if pg.leader == idx {
			pg.leader = -1
		}
		pg.trial++
		pg.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

func (pg *partitionGroup) doOnce(ctx context.Context, fn func(context.Context, pb.KvStoreClient) error) error {
	pg.mu.Lock()
	_, c := pg.pickLocked()
	pg.mu.Unlock()
	return fn(ctx, c)
}

func parseNotLeader(err error) (int32, bool) {
	if err == nil {
		return -1, false
	}
	st, ok := status.FromError(err)
	if !ok {
		return -1, false
	}
	if st.Code() != codes.FailedPrecondition {
		return -1, false
	}
	msg := st.Message()
	if !strings.HasPrefix(msg, notLeaderPrefix) {
		return -1, false
	}
	id, convErr := strconv.Atoi(strings.TrimPrefix(msg, notLeaderPrefix))
	if convErr != nil {
		return -1, false
	}
	return int32(id), true
}

type routingClient struct {
	partitions []*partitionGroup
	n          int
}

var _ pb.KvStoreClient = (*routingClient)(nil)

func partitionFor(key string, n int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}

func (r *routingClient) Put(_ context.Context, in *pb.PutRequest, opts ...grpc.CallOption) (*pb.PutResponse, error) {
	pg := r.partitions[partitionFor(in.Key, r.n)]
	var out *pb.PutResponse
	err := pg.do(func(ctx context.Context, c pb.KvStoreClient) error {
		resp, err := c.Put(ctx, in, opts...)
		if err != nil {
			return err
		}
		out = resp
		return nil
	})
	return out, err
}

func (r *routingClient) Swap(_ context.Context, in *pb.SwapRequest, opts ...grpc.CallOption) (*pb.SwapResponse, error) {
	pg := r.partitions[partitionFor(in.Key, r.n)]
	var out *pb.SwapResponse
	err := pg.do(func(ctx context.Context, c pb.KvStoreClient) error {
		resp, err := c.Swap(ctx, in, opts...)
		if err != nil {
			return err
		}
		out = resp
		return nil
	})
	return out, err
}

func (r *routingClient) Get(_ context.Context, in *pb.GetRequest, opts ...grpc.CallOption) (*pb.GetResponse, error) {
	pg := r.partitions[partitionFor(in.Key, r.n)]
	var out *pb.GetResponse
	err := pg.do(func(ctx context.Context, c pb.KvStoreClient) error {
		resp, err := c.Get(ctx, in, opts...)
		if err != nil {
			return err
		}
		out = resp
		return nil
	})
	return out, err
}

func (r *routingClient) Delete(_ context.Context, in *pb.DeleteRequest, opts ...grpc.CallOption) (*pb.DeleteResponse, error) {
	pg := r.partitions[partitionFor(in.Key, r.n)]
	var out *pb.DeleteResponse
	err := pg.do(func(ctx context.Context, c pb.KvStoreClient) error {
		resp, err := c.Delete(ctx, in, opts...)
		if err != nil {
			return err
		}
		out = resp
		return nil
	})
	return out, err
}

func (r *routingClient) Scan(_ context.Context, in *pb.ScanRequest, opts ...grpc.CallOption) (*pb.ScanResponse, error) {
	var allEntries []*pb.KeyValue
	for i := 0; i < r.n; i++ {
		pg := r.partitions[i]
		err := pg.do(func(ctx context.Context, c pb.KvStoreClient) error {
			resp, err := c.Scan(ctx, in, opts...)
			if err != nil {
				return err
			}
			allEntries = append(allEntries, resp.Entries...)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	sort.Slice(allEntries, func(i, j int) bool {
		return allEntries[i].Key < allEntries[j].Key
	})
	return &pb.ScanResponse{Entries: allEntries}, nil
}

func (r *routingClient) ScanPartial(in *pb.ScanRequest, timeout time.Duration) *pb.ScanResponse {
	var allEntries []*pb.KeyValue
	for i := 0; i < r.n; i++ {
		pg := r.partitions[i]
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		_ = pg.doOnce(ctx, func(ctx context.Context, c pb.KvStoreClient) error {
			resp, err := c.Scan(ctx, in)
			if err != nil {
				return err
			}
			allEntries = append(allEntries, resp.Entries...)
			return nil
		})
		cancel()
	}
	sort.Slice(allEntries, func(i, j int) bool {
		return allEntries[i].Key < allEntries[j].Key
	})
	return &pb.ScanResponse{Entries: allEntries}
}

func (r *routingClient) Close() {
	for _, pg := range r.partitions {
		for _, c := range pg.conns {
			c.Close()
		}
	}
}

func discoverAndConnect(managers []string) *routingClient {
	var layout *pb.DiscoverServersResponse
	for layout == nil {
		for _, addr := range managers {
			conn, err := grpc.NewClient(addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue
			}
			client := pb.NewManagerClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			resp, err := client.DiscoverServers(ctx, &pb.DiscoverServersRequest{})
			cancel()
			conn.Close()
			if err == nil && resp != nil && len(resp.Partitions) > 0 {
				layout = resp
				break
			}
		}
		if layout == nil {
			log.Printf("failed to discover servers from any manager, retrying...")
			time.Sleep(time.Second)
		}
	}

	sort.Slice(layout.Partitions, func(i, j int) bool {
		return layout.Partitions[i].PartitionId < layout.Partitions[j].PartitionId
	})

	rc := &routingClient{
		partitions: make([]*partitionGroup, len(layout.Partitions)),
		n:          len(layout.Partitions),
	}
	for i, p := range layout.Partitions {
		pg := &partitionGroup{leader: -1}
		sort.Slice(p.Replicas, func(a, b int) bool {
			return p.Replicas[a].ReplicaId < p.Replicas[b].ReplicaId
		})
		for _, rep := range p.Replicas {
			conn, err := grpc.NewClient(rep.Address,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatalf("connect %s: %v", rep.Address, err)
			}
			pg.addrs = append(pg.addrs, rep.Address)
			pg.conns = append(pg.conns, conn)
			pg.clients = append(pg.clients, pb.NewKvStoreClient(conn))
		}
		rc.partitions[i] = pg
	}

	log.Printf("discovered %d partitions (rf=%d)", len(layout.Partitions), layout.ReplicationFactor)
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

func pickKeysPerPartition(n, want int) [][]string {
	pool := []string{
		"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet",
		"kilo", "lima", "mike", "november", "oscar", "papa", "quebec", "romeo", "sierra", "tango",
		"uniform", "victor", "whiskey", "xray", "yankee", "zulu",
		"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh", "iii", "jjj",
		"kkk", "lll", "mmm", "nnn", "ooo", "ppp", "qqq", "rrr", "sss", "ttt",
	}
	out := make([][]string, n)
	for _, k := range pool {
		p := partitionFor(k, n)
		if len(out[p]) < want {
			out[p] = append(out[p], k)
		}
	}
	for p := 0; p < n; p++ {
		if len(out[p]) == 0 {
			log.Fatalf("could not find test keys for partition %d", p)
		}
	}
	return out
}

func printPartitionTopology(w *bufio.Writer, rc *routingClient) {
	for p, pg := range rc.partitions {
		pg.mu.Lock()
		fmt.Fprintf(w, "  partition %d: %d replicas\n", p, len(pg.addrs))
		for i, a := range pg.addrs {
			marker := " "
			if i == pg.leader {
				marker = "L"
			}
			fmt.Fprintf(w, "    [%s] replica %d @ %s\n", marker, i, a)
		}
		pg.mu.Unlock()
	}
	w.Flush()
}

func warmLeaderCache(client pb.KvStoreClient, keys [][]string) {
	ctx := context.Background()
	for _, ks := range keys {
		if len(ks) == 0 {
			continue
		}
		_, _ = client.Get(ctx, &pb.GetRequest{Key: ks[0]})
	}
}

func runTestFollowerKill(client pb.KvStoreClient) {
	rc, ok := client.(*routingClient)
	if !ok {
		log.Fatal("test requires --manager_addrs so client can discover partitions")
	}
	writer := bufio.NewWriter(os.Stdout)
	reader := bufio.NewReader(os.Stdin)
	ctx := context.Background()

	waitEnter := func(msg string) {
		fmt.Fprintf(writer, "\n>>> %s\n>>> Press Enter to continue...\n", msg)
		writer.Flush()
		reader.ReadString('\n')
	}

	n := rc.n
	if n < 2 {
		log.Fatalf("follower-kill test needs >=2 partitions, got %d", n)
	}
	rf := len(rc.partitions[0].addrs)
	if rf < 3 {
		log.Fatalf("follower-kill test needs rf>=3 (f>=1), got rf=%d", rf)
	}
	f := (rf - 1) / 2
	keys := pickKeysPerPartition(n, 3)

	fmt.Fprintf(writer, "\n=== Test A: follower failures across >=2 partitions (f=%d per partition) ===\n", f)
	writer.Flush()

	waitEnter(fmt.Sprintf("Step 1: Ensure cluster with n=%d partitions, rf=%d is running", n, rf))

	fmt.Fprintf(writer, "\n=== Step 2: PUT keys across all partitions ===\n")
	values := map[string]string{}
	for p, ks := range keys {
		for _, k := range ks {
			v := "v1_" + k
			if _, err := client.Put(ctx, &pb.PutRequest{Key: k, Value: v}); err != nil {
				log.Fatalf("PUT %s: %v", k, err)
			}
			values[k] = v
			fmt.Fprintf(writer, "  PUT %s=%s (partition %d) OK\n", k, v, p)
		}
	}
	writer.Flush()

	fmt.Fprintf(writer, "\n=== Step 3: GET verify baseline ===\n")
	for p, ks := range keys {
		for _, k := range ks {
			resp, err := client.Get(ctx, &pb.GetRequest{Key: k})
			if err != nil || !resp.Found || resp.Value != values[k] {
				log.Fatalf("GET %s: err=%v found=%v got=%q want=%q", k, err, resp.Found, resp.Value, values[k])
			}
			fmt.Fprintf(writer, "  GET %s=%s (partition %d) OK\n", k, resp.Value, p)
		}
	}
	writer.Flush()

	fmt.Fprintf(writer, "\n=== Step 4: current topology (L = client-believed leader) ===\n")
	printPartitionTopology(writer, rc)

	waitEnter(fmt.Sprintf("Step 5: Kill ONE FOLLOWER in partition 0 AND ONE FOLLOWER in partition 1 (NOT the replicas marked L above). Per-partition kills <= f=%d.", f))

	fmt.Fprintf(writer, "\n=== Step 6: SWAP across all partitions — writes must still commit ===\n")
	for p, ks := range keys {
		for _, k := range ks {
			newV := "v2_" + k
			resp, err := client.Swap(ctx, &pb.SwapRequest{Key: k, Value: newV})
			if err != nil {
				log.Fatalf("SWAP %s: %v", k, err)
			}
			if resp.OldValue != values[k] {
				log.Fatalf("SWAP %s: old=%q, want %q (inconsistent!)", k, resp.OldValue, values[k])
			}
			values[k] = newV
			fmt.Fprintf(writer, "  SWAP %s -> %s (partition %d) OK\n", k, newV, p)
		}
	}
	writer.Flush()

	fmt.Fprintf(writer, "\n=== Step 7: GET verify after follower failures — reads must be consistent ===\n")
	for p, ks := range keys {
		for _, k := range ks {
			resp, err := client.Get(ctx, &pb.GetRequest{Key: k})
			if err != nil || !resp.Found || resp.Value != values[k] {
				log.Fatalf("GET %s: err=%v found=%v got=%q want=%q", k, err, resp.Found, resp.Value, values[k])
			}
			fmt.Fprintf(writer, "  GET %s=%s (partition %d) OK\n", k, resp.Value, p)
		}
	}
	writer.Flush()

	fmt.Fprintf(writer, "\nSTOP\ntestA follower-kill: PASS\n")
	writer.Flush()
}

func runTestLeaderKill(client pb.KvStoreClient) {
	rc, ok := client.(*routingClient)
	if !ok {
		log.Fatal("test requires --manager_addrs so client can discover partitions")
	}
	writer := bufio.NewWriter(os.Stdout)
	reader := bufio.NewReader(os.Stdin)
	ctx := context.Background()

	waitEnter := func(msg string) {
		fmt.Fprintf(writer, "\n>>> %s\n>>> Press Enter to continue...\n", msg)
		writer.Flush()
		reader.ReadString('\n')
	}

	n := rc.n
	rf := len(rc.partitions[0].addrs)
	if rf < 3 {
		log.Fatalf("leader-kill test needs rf>=3 (f>=1), got rf=%d", rf)
	}
	keys := pickKeysPerPartition(n, 2)

	fmt.Fprintf(writer, "\n=== Test B: leader failure + transparent election (rf=%d) ===\n", rf)
	writer.Flush()

	waitEnter(fmt.Sprintf("Step 1: Ensure cluster with n=%d partitions, rf=%d is running", n, rf))

	fmt.Fprintf(writer, "\n=== Step 2: PUT keys across all partitions ===\n")
	values := map[string]string{}
	for p, ks := range keys {
		for _, k := range ks {
			v := "v1_" + k
			if _, err := client.Put(ctx, &pb.PutRequest{Key: k, Value: v}); err != nil {
				log.Fatalf("PUT %s: %v", k, err)
			}
			values[k] = v
			fmt.Fprintf(writer, "  PUT %s=%s (partition %d) OK\n", k, v, p)
		}
	}
	writer.Flush()

	warmLeaderCache(client, keys)

	fmt.Fprintf(writer, "\n=== Step 3: leaders identified by the client ===\n")
	printPartitionTopology(writer, rc)

	targetPart := 0
	pg := rc.partitions[targetPart]
	pg.mu.Lock()
	leaderIdx := pg.leader
	var leaderAddr string
	if leaderIdx >= 0 {
		leaderAddr = pg.addrs[leaderIdx]
	}
	pg.mu.Unlock()
	if leaderIdx < 0 {
		log.Fatalf("partition %d leader still unknown", targetPart)
	}

	waitEnter(fmt.Sprintf("Step 4: Kill the LEADER of partition %d (replica %d @ %s). The surviving replicas should elect a new leader transparently.", targetPart, leaderIdx, leaderAddr))

	fmt.Fprintf(writer, "\n=== Step 5: SWAP on partition %d — may briefly block during election, must succeed ===\n", targetPart)
	writer.Flush()
	for _, k := range keys[targetPart] {
		newV := "v2_" + k
		t0 := time.Now()
		resp, err := client.Swap(ctx, &pb.SwapRequest{Key: k, Value: newV})
		if err != nil {
			log.Fatalf("SWAP %s: %v", k, err)
		}
		if resp.OldValue != values[k] {
			log.Fatalf("SWAP %s: old=%q want %q (inconsistent!)", k, resp.OldValue, values[k])
		}
		values[k] = newV
		fmt.Fprintf(writer, "  SWAP %s -> %s took %v OK\n", k, newV, time.Since(t0))
	}
	writer.Flush()

	fmt.Fprintf(writer, "\n=== Step 6: GET verify across ALL partitions — reads must be consistent ===\n")
	for p, ks := range keys {
		for _, k := range ks {
			resp, err := client.Get(ctx, &pb.GetRequest{Key: k})
			if err != nil || !resp.Found || resp.Value != values[k] {
				log.Fatalf("GET %s: err=%v found=%v got=%q want=%q", k, err, resp.Found, resp.Value, values[k])
			}
			fmt.Fprintf(writer, "  GET %s=%s (partition %d) OK\n", k, resp.Value, p)
		}
	}
	writer.Flush()

	fmt.Fprintf(writer, "\n=== Step 7: post-election topology (new leader for partition %d) ===\n", targetPart)
	printPartitionTopology(writer, rc)

	fmt.Fprintf(writer, "\nSTOP\ntestB leader-kill: PASS\n")
	writer.Flush()
}

func runTestQuorumLoss(client pb.KvStoreClient) {
	rc, ok := client.(*routingClient)
	if !ok {
		log.Fatal("test requires --manager_addrs so client can discover partitions")
	}
	writer := bufio.NewWriter(os.Stdout)
	reader := bufio.NewReader(os.Stdin)
	ctx := context.Background()

	waitEnter := func(msg string) {
		fmt.Fprintf(writer, "\n>>> %s\n>>> Press Enter to continue...\n", msg)
		writer.Flush()
		reader.ReadString('\n')
	}

	n := rc.n
	if n < 2 {
		log.Fatalf("quorum-loss test needs >=2 partitions")
	}
	rf := len(rc.partitions[0].addrs)
	if rf < 3 {
		log.Fatalf("quorum-loss test needs rf>=3, got rf=%d", rf)
	}
	f := (rf - 1) / 2
	needKill := f + 1
	keys := pickKeysPerPartition(n, 2)

	fmt.Fprintf(writer, "\n=== Test C: >f failures in one partition (rf=%d, f=%d, killing %d) ===\n", rf, f, needKill)
	writer.Flush()

	waitEnter(fmt.Sprintf("Step 1: Ensure cluster with n=%d partitions, rf=%d is running", n, rf))

	fmt.Fprintf(writer, "\n=== Step 2: PUT keys across all partitions ===\n")
	values := map[string]string{}
	for p, ks := range keys {
		for _, k := range ks {
			v := "v1_" + k
			if _, err := client.Put(ctx, &pb.PutRequest{Key: k, Value: v}); err != nil {
				log.Fatalf("PUT %s: %v", k, err)
			}
			values[k] = v
			fmt.Fprintf(writer, "  PUT %s=%s (partition %d) OK\n", k, v, p)
		}
	}
	writer.Flush()

	warmLeaderCache(client, keys)

	fmt.Fprintf(writer, "\n=== Step 3: current topology ===\n")
	printPartitionTopology(writer, rc)

	killPart := 0
	waitEnter(fmt.Sprintf("Step 4: Kill %d servers in partition %d (>f=%d → quorum LOST for that partition).", needKill, killPart, f))

	fmt.Fprintf(writer, "\n=== Step 5: GET on SURVIVING partitions must still succeed ===\n")
	for p, ks := range keys {
		if p == killPart {
			continue
		}
		for _, k := range ks {
			resp, err := client.Get(ctx, &pb.GetRequest{Key: k})
			if err != nil || !resp.Found || resp.Value != values[k] {
				log.Fatalf("GET %s: err=%v found=%v got=%q want=%q", k, err, resp.Found, resp.Value, values[k])
			}
			fmt.Fprintf(writer, "  GET %s=%s (partition %d) OK\n", k, resp.Value, p)
		}
	}
	writer.Flush()

	fmt.Fprintf(writer, "\n=== Step 6: GET on partition %d MUST hang/timeout (no stale reads) ===\n", killPart)
	writer.Flush()
	failedKey := keys[killPart][0]
	done := make(chan error, 1)
	go func() {
		_, err := client.Get(ctx, &pb.GetRequest{Key: failedKey})
		done <- err
	}()
	select {
	case err := <-done:
		log.Fatalf("GET %s returned unexpectedly (err=%v) — partition with no quorum must NOT answer", failedKey, err)
	case <-time.After(8 * time.Second):
		fmt.Fprintf(writer, "  GET %s blocked as expected (partition %d has lost quorum)\n", failedKey, killPart)
	}
	writer.Flush()

	fmt.Fprintf(writer, "\n=== Step 7: ScanPartial over a-z — down partition contributes nothing, rest return data ===\n")
	scanResp := rc.ScanPartial(&pb.ScanRequest{KeyStart: "a", KeyEnd: "z"}, 3*time.Second)
	fmt.Fprintf(writer, "  ScanPartial returned %d entries (partition %d omitted)\n", len(scanResp.Entries), killPart)
	writer.Flush()

	fmt.Fprintf(writer, "\nSTOP\ntestC quorum-loss: PASS\n")
	writer.Flush()
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
	managerAddrs := flag.String("manager_addrs", "", "comma-separated list of manager addresses for server discovery")
	managerAlias := flag.String("manager", "", "alias for --manager_addrs")
	server := flag.String("server", "", "server address (single-server mode, bypasses discovery)")
	test := flag.Int("test", 0, "built-in test: 0=stdin, 1=follower-kill, 2=leader-kill, 3=quorum-loss")
	flag.Parse()

	managers := *managerAddrs
	if managers == "" {
		managers = *managerAlias
	}

	var client pb.KvStoreClient
	var cleanup func()

	if managers != "" {
		rc := discoverAndConnect(strings.Split(managers, ","))
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
		log.Fatal("must specify --manager_addrs or --server")
	}
	defer cleanup()

	switch *test {
	case 0:
		runStdin(client)
	case 1:
		runTestFollowerKill(client)
	case 2:
		runTestLeaderKill(client)
	case 3:
		runTestQuorumLoss(client)
	default:
		log.Fatalf("unknown test: %d", *test)
	}
}
