package main

import (
	"context"
	"flag"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"

	pb "kvstore/pb"
)

var syncWrite = &opt.WriteOptions{Sync: true} //leveldb fsync before returning to client

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
	db      *leveldb.DB // nil if no backer path
	logging bool
}

func newKvServer(logging bool, backerPath string) *kvServer {
	s := &kvServer{
		tree:    btree.New(32),
		logging: logging,
	}

	if backerPath != "" {
		db, err := leveldb.OpenFile(backerPath, nil)
		if err != nil {
			log.Fatalf("failed to open LevelDB at %s: %v", backerPath, err)
		}
		s.db = db

		//load all keys from LevelDB
		iter := db.NewIterator(nil, nil)
		count := 0
		for iter.Next() {
			s.tree.ReplaceOrInsert(kvItem{
				key:   string(iter.Key()),
				value: string(iter.Value()),
			})
			count++
		}
		iter.Release()
		if err := iter.Error(); err != nil {
			log.Fatalf("failed to iterate LevelDB: %v", err)
		}

		log.Printf("recovered %d keys from LevelDB", count)
	}

	return s
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

	if s.db != nil { //duh
		if err := s.db.Put([]byte(req.Key), []byte(req.Value), syncWrite); err != nil {
			return nil, err
		}
	}

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

	if s.db != nil { //duh idk why I am not making this a function
		if err := s.db.Put([]byte(req.Key), []byte(req.Value), syncWrite); err != nil {
			return nil, err
		}
	}

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

	//delete from btree FIRST before deleting in leveldb for crash conssitency
	old := s.tree.Delete(kvItem{key: req.Key})
	found := old != nil

	if s.db != nil && found {
		if err := s.db.Delete([]byte(req.Key), syncWrite); err != nil {
			// insert back if db write fail somehow
			s.tree.ReplaceOrInsert(old.(kvItem))
			return nil, err
		}
	}

	if s.logging {
		log.Printf("[%s] DELETE %s -> found=%v", clientAddr(ctx), req.Key, found)
	}
	return &pb.DeleteResponse{Found: found}, nil
}

func registerWithManager(managerAddr string, serverID int) {
	for { //retry LOOP
		conn, err := grpc.NewClient(managerAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("failed to connect to manager: %v, retrying...", err)
			time.Sleep(time.Second)
			continue
		}

		client := pb.NewManagerClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.RegisterServer(ctx, &pb.RegisterServerRequest{
			Id: int32(serverID),
		})
		cancel()
		conn.Close()

		if err != nil {
			log.Printf("failed to register with manager: %v, retrying...", err)
			time.Sleep(time.Second)
			continue
		}

		log.Printf("registered with manager as server %d (accepted=%v)", serverID, resp.Accepted)
		return
	}
}

func main() {
	listen := flag.String("listen", "0.0.0.0:3777", "listen address")
	backer := flag.String("backer", "", "path to durable storage directory")
	logging := flag.Bool("log", false, "enable operation logging")
	id := flag.String("id", "0", "server id")
	manager := flag.String("manager", "", "manager address")
	flag.Parse()

	serverID, err := strconv.Atoi(*id)
	if err != nil {
		log.Fatalf("invalid server id %q: %v", *id, err)
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv := newKvServer(*logging, *backer)
	if srv.db != nil {
		defer srv.db.Close()
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKvStoreServer(grpcServer, srv)

	if *manager != "" {
		go registerWithManager(*manager, serverID)
	}

	log.Printf("server %d listening on %s", serverID, *listen)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
