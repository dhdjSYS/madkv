package main

import (
	"context"
	"flag"
	"log"
	"net"
	"sort"
	"strings"
	"sync"

	"google.golang.org/grpc"

	pb "kvstore/pb"
)

//grader: I use hash partitioning, thus the manager does not share partitioning rules with the server or client, it is all
//calculated by the client given the number of servers

type managerServer struct {
	pb.UnimplementedManagerServer
	mu            sync.Mutex
	expectedAddrs []string
	registered    map[int32]string // id -> address
	allReady      chan struct{}
	readyClosed   bool
}

func newManager(servers []string) *managerServer {
	return &managerServer{
		expectedAddrs: servers,
		registered:    make(map[int32]string),
		allReady:      make(chan struct{}),
	}
}

func (m *managerServer) RegisterServer(_ context.Context, req *pb.RegisterServerRequest) (*pb.RegisterServerResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := req.Id
	if int(id) < len(m.expectedAddrs) {
		m.registered[id] = m.expectedAddrs[id]
	}

	log.Printf("server %d registered (%d/%d)", id, len(m.registered), len(m.expectedAddrs))

	if len(m.registered) == len(m.expectedAddrs) && !m.readyClosed {
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

	m.mu.Lock()
	defer m.mu.Unlock()

	var infos []*pb.ServerInfo
	for id, addr := range m.registered {
		infos = append(infos, &pb.ServerInfo{Id: id, Address: addr})
	}
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Id < infos[j].Id
	})

	return &pb.DiscoverServersResponse{Servers: infos}, nil
}

func main() {
	listen := flag.String("listen", "0.0.0.0:3666", "listen address")
	servers := flag.String("servers", "", "comma-separated server addresses")
	flag.Parse()

	serverList := strings.Split(*servers, ",")

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterManagerServer(grpcServer, newManager(serverList))

	log.Printf("manager listening on %s, expecting %d servers", *listen, len(serverList))
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
