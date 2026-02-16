package main

import (
	"context"
	"flag"
	"log"
	"net"
	"sync"

	"github.com/google/btree"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	pb "kvstore/pb"
)

type kvItem struct {
	key   string
	value string
}

func (a kvItem) Less(b btree.Item) bool {
	return a.key < b.(kvItem).key
}

type kvServer struct {
	pb.UnimplementedKvStoreServer
	mu      sync.RWMutex // RWMutex for better read performance
	tree    *btree.BTree
	logging bool
}

func newKvServer(logging bool) *kvServer {
	return &kvServer{
		tree:    btree.New(32),
		logging: logging,
	}
}

func clientAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	}
	return "unknown"
}

func (s *kvServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item := kvItem{key: req.Key, value: req.Value}
	old := s.tree.ReplaceOrInsert(item)
	found := old != nil
	if s.logging {
		log.Printf("[%s] PUT %s %s -> found=%v", clientAddr(ctx), req.Key, req.Value, found)
	}
	return &pb.PutResponse{Found: found}, nil
}

func (s *kvServer) Swap(ctx context.Context, req *pb.SwapRequest) (*pb.SwapResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item := kvItem{key: req.Key, value: req.Value}
	old := s.tree.ReplaceOrInsert(item)
	resp := &pb.SwapResponse{}
	if old != nil {
		resp.Found = true
		resp.OldValue = old.(kvItem).value
	}
	if s.logging {
		log.Printf("[%s] SWAP %s %s -> found=%v old_value=%s", clientAddr(ctx), req.Key, req.Value, resp.Found, resp.OldValue)
	}
	return resp, nil
}

func (s *kvServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item := s.tree.Get(kvItem{key: req.Key})
	resp := &pb.GetResponse{}
	if item != nil {
		resp.Found = true
		resp.Value = item.(kvItem).value
	}
	if s.logging {
		log.Printf("[%s] GET %s -> found=%v value=%s", clientAddr(ctx), req.Key, resp.Found, resp.Value)
	}
	return resp, nil
}

func (s *kvServer) Scan(ctx context.Context, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var entries []*pb.KeyValue
	s.tree.AscendGreaterOrEqual(kvItem{key: req.KeyStart}, func(i btree.Item) bool {
		kv := i.(kvItem)
		if kv.key > req.KeyEnd {
			return false
		}
		entries = append(entries, &pb.KeyValue{Key: kv.key, Value: kv.value})
		return true
	})
	if s.logging {
		log.Printf("[%s] SCAN %s %s -> %d entries", clientAddr(ctx), req.KeyStart, req.KeyEnd, len(entries))
	}
	return &pb.ScanResponse{Entries: entries}, nil
}

func (s *kvServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.tree.Delete(kvItem{key: req.Key})
	found := old != nil
	if s.logging {
		log.Printf("[%s] DELETE %s -> found=%v", clientAddr(ctx), req.Key, found)
	}
	return &pb.DeleteResponse{Found: found}, nil
}

func main() {
	listen := flag.String("listen", "0.0.0.0:3777", "listen address")
	// debug
	logging := flag.Bool("log", false, "enable operation logging")
	flag.Parse()

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKvStoreServer(grpcServer, newKvServer(*logging))

	log.Printf("server listening on %s", *listen)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
