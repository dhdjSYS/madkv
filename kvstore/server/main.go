package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "kvstore/pb"
	"kvstore/raft"
)

type kvItem struct {
	key   string
	value string
}

func (a kvItem) Less(b btree.Item) bool {
	return a.key < b.(kvItem).key
}

type kvStateMachine struct {
	tree *btree.BTree
}

func newKVStateMachine() *kvStateMachine {
	return &kvStateMachine{tree: btree.New(32)}
}

func (s *kvStateMachine) Apply(cmdBytes []byte) []byte {
	cmd := &pb.KVCommand{}
	if err := proto.Unmarshal(cmdBytes, cmd); err != nil {
		log.Printf("state machine: bad command: %v", err)
		return nil
	}
	res := &pb.KVResult{}
	switch cmd.Op {
	case pb.KVCommand_OP_PUT:
		old := s.tree.ReplaceOrInsert(kvItem{key: cmd.Key, value: cmd.Value})
		res.Found = old != nil
	case pb.KVCommand_OP_SWAP:
		old := s.tree.ReplaceOrInsert(kvItem{key: cmd.Key, value: cmd.Value})
		if old != nil {
			res.Found = true
			res.Value = old.(kvItem).value
		}
	case pb.KVCommand_OP_DELETE:
		old := s.tree.Delete(kvItem{key: cmd.Key})
		res.Found = old != nil
	case pb.KVCommand_OP_GET:
		item := s.tree.Get(kvItem{key: cmd.Key})
		if item != nil {
			res.Found = true
			res.Value = item.(kvItem).value
		}
	case pb.KVCommand_OP_SCAN:
		s.tree.AscendGreaterOrEqual(kvItem{key: cmd.Key}, func(i btree.Item) bool {
			kv := i.(kvItem)
			if kv.key > cmd.KeyEnd {
				return false
			}
			res.Entries = append(res.Entries, &pb.KeyValue{Key: kv.key, Value: kv.value})
			return true
		})
	default:
		log.Printf("state machine: unknown op %v", cmd.Op)
	}
	data, err := proto.Marshal(res)
	if err != nil {
		log.Printf("state machine: marshal: %v", err)
		return nil
	}
	return data
}

const notLeaderPrefix = "NOT_LEADER:"

type kvServer struct {
	pb.UnimplementedKvStoreServer
	raft *raft.Raft
}

func (s *kvServer) notLeaderErr() error {
	return status.Errorf(codes.FailedPrecondition, "%s%d", notLeaderPrefix, s.raft.LeaderID())
}

func (s *kvServer) proposeAndWait(cmd *pb.KVCommand) (*pb.KVResult, error) {
	data, err := proto.Marshal(cmd)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal: %v", err)
	}
	ch, err := s.raft.Propose(data)
	if err != nil {
		if err == raft.ErrNotLeader {
			return nil, s.notLeaderErr()
		}
		return nil, status.Errorf(codes.Internal, "raft: %v", err)
	}
	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, s.notLeaderErr()
		}
		kvRes := &pb.KVResult{}
		if err := proto.Unmarshal(res.Data, kvRes); err != nil {
			return nil, status.Errorf(codes.Internal, "unmarshal: %v", err)
		}
		return kvRes, nil
	case <-time.After(10 * time.Second):
		return nil, status.Errorf(codes.DeadlineExceeded, "raft apply timeout")
	}
}

func (s *kvServer) Put(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	res, err := s.proposeAndWait(&pb.KVCommand{
		Op: pb.KVCommand_OP_PUT, Key: req.Key, Value: req.Value,
	})
	if err != nil {
		return nil, err
	}
	return &pb.PutResponse{Found: res.Found}, nil
}

func (s *kvServer) Swap(_ context.Context, req *pb.SwapRequest) (*pb.SwapResponse, error) {
	res, err := s.proposeAndWait(&pb.KVCommand{
		Op: pb.KVCommand_OP_SWAP, Key: req.Key, Value: req.Value,
	})
	if err != nil {
		return nil, err
	}
	return &pb.SwapResponse{Found: res.Found, OldValue: res.Value}, nil
}

func (s *kvServer) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	res, err := s.proposeAndWait(&pb.KVCommand{
		Op: pb.KVCommand_OP_GET, Key: req.Key,
	})
	if err != nil {
		return nil, err
	}
	return &pb.GetResponse{Found: res.Found, Value: res.Value}, nil
}

func (s *kvServer) Delete(_ context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	res, err := s.proposeAndWait(&pb.KVCommand{
		Op: pb.KVCommand_OP_DELETE, Key: req.Key,
	})
	if err != nil {
		return nil, err
	}
	return &pb.DeleteResponse{Found: res.Found}, nil
}

func (s *kvServer) Scan(_ context.Context, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	res, err := s.proposeAndWait(&pb.KVCommand{
		Op: pb.KVCommand_OP_SCAN, Key: req.KeyStart, KeyEnd: req.KeyEnd,
	})
	if err != nil {
		return nil, err
	}
	return &pb.ScanResponse{Entries: res.Entries}, nil
}

func registerWithManagers(managers []string, partID, repID int32) {
	req := &pb.RegisterServerRequest{PartitionId: partID, ReplicaId: repID}
	for attempt := 0; ; attempt++ {
		for _, addr := range managers {
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue
			}
			client := pb.NewManagerClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err = client.RegisterServer(ctx, req)
			cancel()
			conn.Close()
			if err == nil {
				log.Printf("registered with manager %s as p=%d r=%d", addr, partID, repID)
				return
			}
		}
		time.Sleep(time.Second)
	}
}

func parsePeerAddrs(raw string, myID int32) (map[int32]string, int) {
	if raw == "" || raw == "none" {
		return map[int32]string{}, 1
	}
	parts := strings.Split(raw, ",")
	rf := len(parts) + 1
	out := make(map[int32]string, len(parts))
	j := 0
	for i := 0; i < rf; i++ {
		if int32(i) == myID {
			continue
		}
		out[int32(i)] = strings.TrimSpace(parts[j])
		j++
	}
	return out, rf
}

func main() {
	partitionID := flag.Int("partition_id", 0, "partition index")
	replicaID := flag.Int("replica_id", 0, "replica index within the partition")
	managerAddrs := flag.String("manager_addrs", "", "comma-separated manager API addresses")
	apiListen := flag.String("api_listen", "0.0.0.0:3777", "client KV API listen address")
	p2pListen := flag.String("p2p_listen", "0.0.0.0:3707", "raft peer listen address")
	peerAddrs := flag.String("peer_addrs", "none", "comma-separated peer p2p addresses ('none' if rf=1)")
	backerPath := flag.String("backer_path", "", "durable storage directory for this replica")
	flag.Parse()

	if *backerPath == "" {
		log.Fatal("--backer_path is required")
	}
	partID := int32(*partitionID)
	repID := int32(*replicaID)

	peerMap, rf := parsePeerAddrs(*peerAddrs, repID)
	log.Printf("partition=%d replica=%d rf=%d peers=%v backer=%s",
		partID, repID, rf, peerMap, *backerPath)

	apiLis, err := net.Listen("tcp", *apiListen)
	if err != nil {
		log.Fatalf("listen api %s: %v", *apiListen, err)
	}
	p2pLis, err := net.Listen("tcp", *p2pListen)
	if err != nil {
		log.Fatalf("listen p2p %s: %v", *p2pListen, err)
	}

	sm := newKVStateMachine()

	logger := log.New(log.Writer(), fmt.Sprintf("[raft p=%d r=%d] ", partID, repID), log.LstdFlags|log.Lmicroseconds)
	rft, err := raft.NewRaft(raft.Config{
		ID:         repID,
		NumPeers:   rf,
		PeerAddrs:  peerMap,
		BackerPath: *backerPath,
		SM:         sm,
		Logger:     logger,
	})
	if err != nil {
		log.Fatalf("raft init: %v", err)
	}

	kvSrv := &kvServer{raft: rft}

	p2pGRPC := grpc.NewServer()
	pb.RegisterRaftServer(p2pGRPC, rft)
	go func() {
		if err := p2pGRPC.Serve(p2pLis); err != nil {
			log.Printf("p2p serve: %v", err)
		}
	}()

	rft.Start()

	if *managerAddrs != "" {
		managers := strings.Split(*managerAddrs, ",")
		go registerWithManagers(managers, partID, repID)
	}

	apiGRPC := grpc.NewServer()
	pb.RegisterKvStoreServer(apiGRPC, kvSrv)
	log.Printf("kvserver p=%d r=%d api=%s p2p=%s", partID, repID, *apiListen, *p2pListen)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := apiGRPC.Serve(apiLis); err != nil {
			log.Printf("api serve: %v", err)
		}
	}()
	wg.Wait()
}
