package raft

import (
	"context"
	"errors"
	"fmt"
	"log"
	mrand "math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "kvstore/pb"
)

type StateMachine interface {
	Apply(cmd []byte) []byte
}

type Result struct {
	Data []byte
	Err  error
}

var ErrNotLeader = errors.New("raft: not leader")

type role int

const (
	follower role = iota
	preCandidate
	candidate
	leader
)

func (r role) String() string {
	switch r {
	case follower:
		return "follower"
	case preCandidate:
		return "pre-candidate"
	case candidate:
		return "candidate"
	case leader:
		return "leader"
	}
	return "?"
}

const (
	electionTimeoutMin = 800 * time.Millisecond
	electionTimeoutMax = 1600 * time.Millisecond
	heartbeatInterval  = 120 * time.Millisecond
	rpcTimeout         = 600 * time.Millisecond
	maxEntriesPerAE    = 128
)

type Config struct {
	ID         int32
	NumPeers   int
	PeerAddrs  map[int32]string
	BackerPath string
	SM         StateMachine
	Logger     *log.Logger
}

type proposalWaiter struct {
	term    int64
	resultC chan Result
}

type Raft struct {
	pb.UnimplementedRaftServer

	id        int32
	numPeers  int
	peerAddrs map[int32]string
	sm        StateMachine
	logger    *log.Logger

	mu sync.Mutex

	currentTerm int64
	votedFor    int32
	log         []*pb.LogEntry

	commitIndex int64
	lastApplied int64
	role        role
	leaderId    int32

	nextIndex  map[int32]int64
	matchIndex map[int32]int64

	electionResetAt time.Time
	electionTimeout time.Duration
	lastLeaderHeard time.Time
	rng             *mrand.Rand

	applyCond *sync.Cond

	peerSignals map[int32]chan struct{}

	waiters map[int64]*proposalWaiter

	persister *persister
	transport *transport

	shutdown     chan struct{}
	shutdownOnce sync.Once
}

func NewRaft(cfg Config) (*Raft, error) {
	if cfg.NumPeers <= 0 {
		return nil, fmt.Errorf("raft: NumPeers must be positive")
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	if cfg.SM == nil {
		return nil, fmt.Errorf("raft: nil state machine")
	}

	p, err := openPersister(cfg.BackerPath)
	if err != nil {
		return nil, err
	}
	term, voted, commit, logEntries, err := p.loadState()
	if err != nil {
		p.close()
		return nil, err
	}
	if maxIdx := int64(len(logEntries)) - 1; commit > maxIdx {
		commit = maxIdx
	}

	r := &Raft{
		id:          cfg.ID,
		numPeers:    cfg.NumPeers,
		peerAddrs:   cfg.PeerAddrs,
		sm:          cfg.SM,
		logger:      cfg.Logger,
		currentTerm: term,
		votedFor:    voted,
		log:         logEntries,
		commitIndex: commit,
		role:        follower,
		leaderId:    -1,
		nextIndex:   make(map[int32]int64),
		matchIndex:  make(map[int32]int64),
		peerSignals: make(map[int32]chan struct{}),
		waiters:     make(map[int64]*proposalWaiter),
		persister:   p,
		rng:         mrand.New(mrand.NewPCG(uint64(time.Now().UnixNano()), uint64(cfg.ID)+1)),
		shutdown:    make(chan struct{}),
	}
	r.applyCond = sync.NewCond(&r.mu)
	for pid := range cfg.PeerAddrs {
		r.peerSignals[pid] = make(chan struct{}, 1)
	}
	r.transport = newTransport(cfg.PeerAddrs)
	r.resetElectionTimerLocked()

	r.logger.Printf("raft id=%d rf=%d term=%d voted=%d logLen=%d commit=%d",
		r.id, r.numPeers, r.currentTerm, r.votedFor, len(r.log)-1, r.commitIndex)
	return r, nil
}

func (r *Raft) Start() {
	go r.electionLoop()
	go r.applyLoop()
	for pid := range r.peerAddrs {
		go r.peerLoop(pid)
	}
}

func (r *Raft) Stop() {
	r.shutdownOnce.Do(func() {
		close(r.shutdown)
		r.mu.Lock()
		r.applyCond.Broadcast()
		r.mu.Unlock()
		r.transport.close()
		r.persister.close()
	})
}

func (r *Raft) isShutdown() bool {
	select {
	case <-r.shutdown:
		return true
	default:
		return false
	}
}

func (r *Raft) LeaderID() int32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderId
}

func (r *Raft) Propose(cmd []byte) (<-chan Result, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.role != leader {
		return nil, ErrNotLeader
	}

	index := int64(len(r.log))
	entry := &pb.LogEntry{
		Index:   index,
		Term:    r.currentTerm,
		Command: cmd,
	}
	r.log = append(r.log, entry)
	if err := r.persister.appendEntries([]*pb.LogEntry{entry}); err != nil {
		r.log = r.log[:len(r.log)-1]
		return nil, fmt.Errorf("raft: persist: %w", err)
	}

	ch := make(chan Result, 1)
	r.waiters[index] = &proposalWaiter{term: r.currentTerm, resultC: ch}

	for pid := range r.peerAddrs {
		r.signalPeerLocked(pid)
	}
	r.advanceCommitIndexLocked()
	return ch, nil
}

func (r *Raft) resetElectionTimerLocked() {
	r.electionResetAt = time.Now()
	span := int64(electionTimeoutMax - electionTimeoutMin)
	r.electionTimeout = electionTimeoutMin + time.Duration(r.rng.Int64N(span))
}

func (r *Raft) lastLogInfoLocked() (int64, int64) {
	last := r.log[len(r.log)-1]
	return last.Index, last.Term
}

func (r *Raft) signalPeerLocked(pid int32) {
	select {
	case r.peerSignals[pid] <- struct{}{}:
	default:
	}
}

func (r *Raft) becomeFollowerLocked(term int64) {
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = -1
		if err := r.persister.saveMeta(r.currentTerm, r.votedFor); err != nil {
			r.logger.Printf("raft persist meta: %v", err)
		}
	}
	if r.role != follower {
		r.logger.Printf("raft id=%d -> follower term=%d", r.id, r.currentTerm)
	}
	r.role = follower
	r.leaderId = -1
}

func (r *Raft) becomeLeaderLocked() {
	if r.role == leader {
		return
	}
	r.role = leader
	r.leaderId = r.id
	r.lastLeaderHeard = time.Now()
	lastIdx, _ := r.lastLogInfoLocked()

	r.nextIndex = make(map[int32]int64, len(r.peerAddrs))
	r.matchIndex = make(map[int32]int64, len(r.peerAddrs))
	for pid := range r.peerAddrs {
		r.nextIndex[pid] = lastIdx + 1
		r.matchIndex[pid] = 0
	}

	noop := &pb.LogEntry{
		Index:   lastIdx + 1,
		Term:    r.currentTerm,
		Command: nil,
	}
	r.log = append(r.log, noop)
	if err := r.persister.appendEntries([]*pb.LogEntry{noop}); err != nil {
		r.logger.Printf("raft persist noop: %v", err)
	}

	r.logger.Printf("raft id=%d became LEADER term=%d lastIdx=%d",
		r.id, r.currentTerm, lastIdx+1)

	for pid := range r.peerAddrs {
		r.signalPeerLocked(pid)
	}
	r.advanceCommitIndexLocked()
}

func (r *Raft) advanceCommitIndexLocked() {
	if r.role != leader {
		return
	}
	lastIdx := int64(len(r.log)) - 1
	for N := lastIdx; N > r.commitIndex; N-- {
		if r.log[N].Term != r.currentTerm {
			continue
		}
		count := 1
		for _, m := range r.matchIndex {
			if m >= N {
				count++
			}
		}
		if count*2 > r.numPeers {
			r.commitIndex = N
			if err := r.persister.saveCommit(r.commitIndex); err != nil {
				r.logger.Printf("raft persist commit: %v", err)
			}
			r.applyCond.Broadcast()
			return
		}
	}
}

func (r *Raft) electionLoop() {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-r.shutdown:
			return
		case <-tick.C:
		}
		r.mu.Lock()
		if r.role != leader && time.Since(r.electionResetAt) >= r.electionTimeout {
			r.startPreElectionLocked()
		}
		r.mu.Unlock()
	}
}

func (r *Raft) startPreElectionLocked() {
	r.role = preCandidate
	r.resetElectionTimerLocked()

	hypTerm := r.currentTerm + 1
	termAtStart := r.currentTerm
	lastIdx, lastTerm := r.lastLogInfoLocked()
	r.logger.Printf("raft id=%d pre-election hypTerm=%d lastIdx=%d lastTerm=%d",
		r.id, hypTerm, lastIdx, lastTerm)

	votes := 1
	if votes*2 > r.numPeers {
		r.startElectionLocked()
		return
	}

	for pid := range r.peerAddrs {
		go func(pid int32) {
			req := &pb.RequestVoteRequest{
				Term:         hypTerm,
				CandidateId:  r.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
				PreVote:      true,
			}
			resp, err := r.transport.requestVote(pid, req)
			if err != nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.role != preCandidate || r.currentTerm != termAtStart {
				return
			}
			if resp.Term > r.currentTerm {
				r.becomeFollowerLocked(resp.Term)
				return
			}
			if resp.VoteGranted {
				votes++
				if votes*2 > r.numPeers && r.role == preCandidate {
					r.startElectionLocked()
				}
			}
		}(pid)
	}
}

func (r *Raft) startElectionLocked() {
	r.role = candidate
	r.currentTerm++
	r.votedFor = r.id
	r.leaderId = -1
	if err := r.persister.saveMeta(r.currentTerm, r.votedFor); err != nil {
		r.logger.Printf("raft persist meta: %v", err)
	}
	r.resetElectionTimerLocked()

	electionTerm := r.currentTerm
	lastIdx, lastTerm := r.lastLogInfoLocked()
	r.logger.Printf("raft id=%d election term=%d lastIdx=%d lastTerm=%d",
		r.id, electionTerm, lastIdx, lastTerm)

	votes := 1
	if votes*2 > r.numPeers {
		r.becomeLeaderLocked()
		return
	}

	for pid := range r.peerAddrs {
		go func(pid int32) {
			req := &pb.RequestVoteRequest{
				Term:         electionTerm,
				CandidateId:  r.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			resp, err := r.transport.requestVote(pid, req)
			if err != nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.role != candidate || r.currentTerm != electionTerm {
				return
			}
			if resp.Term > r.currentTerm {
				r.becomeFollowerLocked(resp.Term)
				return
			}
			if resp.VoteGranted {
				votes++
				if votes*2 > r.numPeers && r.role == candidate {
					r.becomeLeaderLocked()
				}
			}
		}(pid)
	}
}

func (r *Raft) applyLoop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		for !r.isShutdown() && r.commitIndex <= r.lastApplied {
			r.applyCond.Wait()
		}
		if r.isShutdown() {
			return
		}
		for r.lastApplied < r.commitIndex {
			r.lastApplied++
			entry := r.log[r.lastApplied]
			var result []byte
			if len(entry.Command) > 0 {
				result = r.sm.Apply(entry.Command)
			}
			if w, ok := r.waiters[r.lastApplied]; ok {
				if w.term == entry.Term {
					w.resultC <- Result{Data: result}
				} else {
					w.resultC <- Result{Err: ErrNotLeader}
				}
				delete(r.waiters, r.lastApplied)
			}
		}
	}
}

func (r *Raft) peerLoop(pid int32) {
	sig := r.peerSignals[pid]
	timer := time.NewTimer(heartbeatInterval)
	defer timer.Stop()

	for {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(heartbeatInterval)

		select {
		case <-r.shutdown:
			return
		case <-sig:
		case <-timer.C:
		}

		r.mu.Lock()
		if r.role != leader {
			r.mu.Unlock()
			continue
		}
		term := r.currentTerm
		nextIdx := r.nextIndex[pid]
		if nextIdx < 1 {
			nextIdx = 1
		}
		prevIdx := nextIdx - 1
		prevTerm := r.log[prevIdx].Term
		var entries []*pb.LogEntry
		lastLogIdx := int64(len(r.log)) - 1
		limit := nextIdx + maxEntriesPerAE
		for i := nextIdx; i <= lastLogIdx && i < limit; i++ {
			entries = append(entries, r.log[i])
		}
		leaderCommit := r.commitIndex
		r.mu.Unlock()

		req := &pb.AppendEntriesRequest{
			Term:         term,
			LeaderId:     r.id,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}
		resp, err := r.transport.appendEntries(pid, req)
		if err != nil {
			continue
		}

		r.mu.Lock()
		if r.role != leader || r.currentTerm != term {
			r.mu.Unlock()
			continue
		}
		if resp.Term > r.currentTerm {
			r.becomeFollowerLocked(resp.Term)
			r.mu.Unlock()
			continue
		}
		if resp.Success {
			newMatch := prevIdx + int64(len(entries))
			if newMatch > r.matchIndex[pid] {
				r.matchIndex[pid] = newMatch
			}
			r.nextIndex[pid] = newMatch + 1
			r.advanceCommitIndexLocked()
			if r.nextIndex[pid] <= int64(len(r.log))-1 {
				r.signalPeerLocked(pid)
			}
		} else {
			if resp.ConflictIndex > 0 && resp.ConflictIndex < r.nextIndex[pid] {
				r.nextIndex[pid] = resp.ConflictIndex
			} else if r.nextIndex[pid] > 1 {
				r.nextIndex[pid]--
			}
			r.signalPeerLocked(pid)
		}
		r.mu.Unlock()
	}
}

func (r *Raft) RequestVote(_ context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &pb.RequestVoteResponse{Term: r.currentTerm}

	if req.Term < r.currentTerm {
		return resp, nil
	}

	liveLeader := r.role == leader ||
		(r.leaderId >= 0 && time.Since(r.lastLeaderHeard) < electionTimeoutMin)
	if liveLeader {
		return resp, nil
	}

	lastIdx, lastTerm := r.lastLogInfoLocked()
	upToDate := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx)

	if req.PreVote {
		if upToDate {
			resp.VoteGranted = true
		}
		return resp, nil
	}

	if req.Term > r.currentTerm {
		r.becomeFollowerLocked(req.Term)
	}
	resp.Term = r.currentTerm

	if (r.votedFor == -1 || r.votedFor == req.CandidateId) && upToDate {
		r.votedFor = req.CandidateId
		if err := r.persister.saveMeta(r.currentTerm, r.votedFor); err != nil {
			r.logger.Printf("raft persist meta: %v", err)
		}
		r.resetElectionTimerLocked()
		resp.VoteGranted = true
	}
	return resp, nil
}

func (r *Raft) AppendEntries(_ context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &pb.AppendEntriesResponse{Term: r.currentTerm, Success: false}

	if req.Term < r.currentTerm {
		return resp, nil
	}

	if req.Term > r.currentTerm {
		r.becomeFollowerLocked(req.Term)
	} else if r.role == candidate || r.role == preCandidate {
		r.role = follower
	}
	r.leaderId = req.LeaderId
	r.lastLeaderHeard = time.Now()
	r.resetElectionTimerLocked()
	resp.Term = r.currentTerm

	lastIdx := int64(len(r.log)) - 1
	if req.PrevLogIndex > lastIdx {
		resp.ConflictIndex = lastIdx + 1
		return resp, nil
	}
	if req.PrevLogIndex >= 0 && r.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		conflictTerm := r.log[req.PrevLogIndex].Term
		ci := req.PrevLogIndex
		for ci > 0 && r.log[ci-1].Term == conflictTerm {
			ci--
		}
		resp.ConflictIndex = ci
		return resp, nil
	}

	newStart := 0
	insertBase := req.PrevLogIndex + 1
	for newStart < len(req.Entries) {
		idx := insertBase + int64(newStart)
		if idx > int64(len(r.log))-1 {
			break
		}
		if r.log[idx].Term != req.Entries[newStart].Term {
			truncFrom := idx
			truncTo := int64(len(r.log)) - 1
			for i := truncFrom; i <= truncTo; i++ {
				if w, ok := r.waiters[i]; ok {
					w.resultC <- Result{Err: ErrNotLeader}
					delete(r.waiters, i)
				}
			}
			r.log = r.log[:truncFrom]
			if err := r.persister.deleteLogFrom(truncFrom, truncTo); err != nil {
				r.logger.Printf("raft persist truncate: %v", err)
			}
			break
		}
		newStart++
	}
	if newStart < len(req.Entries) {
		toAppend := req.Entries[newStart:]
		r.log = append(r.log, toAppend...)
		if err := r.persister.appendEntries(toAppend); err != nil {
			r.logger.Printf("raft persist append: %v", err)
		}
	}

	if req.LeaderCommit > r.commitIndex {
		newCommit := req.LeaderCommit
		lastNewIdx := int64(len(r.log)) - 1
		if lastNewIdx < newCommit {
			newCommit = lastNewIdx
		}
		if newCommit > r.commitIndex {
			r.commitIndex = newCommit
			if err := r.persister.saveCommit(r.commitIndex); err != nil {
				r.logger.Printf("raft persist commit: %v", err)
			}
			r.applyCond.Broadcast()
		}
	}

	resp.Success = true
	return resp, nil
}

type transport struct {
	mu      sync.Mutex
	addrs   map[int32]string
	conns   map[int32]*grpc.ClientConn
	clients map[int32]pb.RaftClient
}

func newTransport(peerAddrs map[int32]string) *transport {
	addrs := make(map[int32]string, len(peerAddrs))
	for k, v := range peerAddrs {
		addrs[k] = v
	}
	return &transport{
		addrs:   addrs,
		conns:   make(map[int32]*grpc.ClientConn),
		clients: make(map[int32]pb.RaftClient),
	}
}

func (t *transport) client(pid int32) (pb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.clients[pid]; ok {
		return c, nil
	}
	addr, ok := t.addrs[pid]
	if !ok {
		return nil, fmt.Errorf("raft transport: unknown peer %d", pid)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	t.conns[pid] = conn
	client := pb.NewRaftClient(conn)
	t.clients[pid] = client
	return client, nil
}

func (t *transport) requestVote(pid int32, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	c, err := t.client(pid)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	return c.RequestVote(ctx, req)
}

func (t *transport) appendEntries(pid int32, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	c, err := t.client(pid)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	return c.AppendEntries(ctx, req)
}

func (t *transport) close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		c.Close()
	}
	t.conns = nil
	t.clients = nil
}
