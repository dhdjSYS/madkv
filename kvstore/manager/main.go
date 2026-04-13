package main

import (
	"context"
	"flag"
	"log"
	"net"
	"strings"
	"sync"

	"google.golang.org/grpc"

	pb "kvstore/pb"
)

type managerServer struct {
	pb.UnimplementedManagerServer

	rf            int32
	numPartitions int
	serverAddrs   []string

	mu          sync.Mutex
	registered  map[[2]int32]bool
	allReady    chan struct{}
	readyClosed bool
}

func newManager(rf int32, servers []string) *managerServer {
	return &managerServer{
		rf:            rf,
		numPartitions: len(servers) / int(rf),
		serverAddrs:   servers,
		registered:    make(map[[2]int32]bool),
		allReady:      make(chan struct{}),
	}
}

func (m *managerServer) RegisterServer(_ context.Context, req *pb.RegisterServerRequest) (*pb.RegisterServerResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := [2]int32{req.PartitionId, req.ReplicaId}
	if !m.registered[key] {
		m.registered[key] = true
		log.Printf("register p=%d r=%d (%d/%d)",
			req.PartitionId, req.ReplicaId, len(m.registered), len(m.serverAddrs))
	} else {
		log.Printf("re-register p=%d r=%d (recovery)",
			req.PartitionId, req.ReplicaId)
	}
	if len(m.registered) >= len(m.serverAddrs) && !m.readyClosed {
		close(m.allReady)
		m.readyClosed = true
	}
	return &pb.RegisterServerResponse{Accepted: true}, nil
}

func (m *managerServer) DiscoverServers(ctx context.Context, _ *pb.DiscoverServersRequest) (*pb.DiscoverServersResponse, error) {
	select {
	case <-m.allReady:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	groups := make([]*pb.PartitionGroup, m.numPartitions)
	for p := 0; p < m.numPartitions; p++ {
		group := &pb.PartitionGroup{PartitionId: int32(p)}
		for r := int32(0); r < m.rf; r++ {
			idx := p*int(m.rf) + int(r)
			group.Replicas = append(group.Replicas, &pb.ServerInfo{
				ReplicaId: r,
				Address:   m.serverAddrs[idx],
			})
		}
		groups[p] = group
	}
	return &pb.DiscoverServersResponse{
		NumPartitions:     int32(m.numPartitions),
		ReplicationFactor: m.rf,
		Partitions:        groups,
	}, nil
}

func main() {
	replicaID := flag.Int("replica_id", 0, "manager replica index (must be 0 in non-replicated mode)")
	manListen := flag.String("man_listen", "0.0.0.0:3666", "client/server API listen address")
	p2pListen := flag.String("p2p_listen", "0.0.0.0:3606", "manager peer listen address (ignored in non-replicated mode)")
	peerAddrs := flag.String("peer_addrs", "none", "comma-separated manager peers, or 'none' (ignored in non-replicated mode)")
	serverRF := flag.Int("server_rf", 1, "replication factor of data servers")
	serverAddrs := flag.String("server_addrs", "", "comma-separated server API addresses: partition-major ordering")
	backerPath := flag.String("backer_path", "", "durable storage directory (unused in non-replicated mode)")
	flag.Parse()

	_ = p2pListen
	_ = peerAddrs
	_ = backerPath

	if *replicaID != 0 {
		log.Printf("manager replica %d: non-replicated mode, idling", *replicaID)
		select {}
	}

	if *serverAddrs == "" {
		log.Fatal("--server_addrs is required")
	}
	servers := strings.Split(*serverAddrs, ",")
	if *serverRF <= 0 || len(servers)%*serverRF != 0 {
		log.Fatalf("server count %d not a multiple of server_rf %d", len(servers), *serverRF)
	}

	lis, err := net.Listen("tcp", *manListen)
	if err != nil {
		log.Fatalf("listen %s: %v", *manListen, err)
	}

	mgr := newManager(int32(*serverRF), servers)
	grpcServer := grpc.NewServer()
	pb.RegisterManagerServer(grpcServer, mgr)

	log.Printf("manager listening on %s, %d partitions rf=%d (%d servers)",
		*manListen, mgr.numPartitions, mgr.rf, len(servers))
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
