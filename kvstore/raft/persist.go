package raft

import (
	"encoding/binary"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"google.golang.org/protobuf/proto"

	pb "kvstore/pb"
)

var (
	metaTermKey   = []byte("meta:term")
	metaVotedKey  = []byte("meta:voted")
	metaCommitKey = []byte("meta:commit")
	logPrefix     = []byte("log:")
)

type persister struct {
	db *leveldb.DB
}

func openPersister(path string) (*persister, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, fmt.Errorf("raft persister open %s: %w", path, err)
	}
	return &persister{db: db}, nil
}

func (p *persister) close() error {
	return p.db.Close()
}

func logKey(idx int64) []byte {
	buf := make([]byte, len(logPrefix)+8)
	copy(buf, logPrefix)
	binary.BigEndian.PutUint64(buf[len(logPrefix):], uint64(idx))
	return buf
}

func encodeInt64(v int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return buf
}

func decodeInt64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

func encodeInt32(v int32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(v))
	return buf
}

func decodeInt32(b []byte) int32 {
	return int32(binary.BigEndian.Uint32(b))
}

func (p *persister) saveMeta(term int64, votedFor int32) error {
	batch := new(leveldb.Batch)
	batch.Put(metaTermKey, encodeInt64(term))
	batch.Put(metaVotedKey, encodeInt32(votedFor))
	return p.db.Write(batch, nil)
}

func (p *persister) saveCommit(commit int64) error {
	return p.db.Put(metaCommitKey, encodeInt64(commit), nil)
}

func (p *persister) appendEntries(entries []*pb.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	batch := new(leveldb.Batch)
	for _, e := range entries {
		data, err := proto.Marshal(e)
		if err != nil {
			return err
		}
		batch.Put(logKey(e.Index), data)
	}
	return p.db.Write(batch, nil)
}

func (p *persister) deleteLogFrom(startIdx, endIdx int64) error {
	if endIdx < startIdx {
		return nil
	}
	batch := new(leveldb.Batch)
	for i := startIdx; i <= endIdx; i++ {
		batch.Delete(logKey(i))
	}
	return p.db.Write(batch, nil)
}

func (p *persister) loadState() (term int64, votedFor int32, commit int64, log []*pb.LogEntry, err error) {
	votedFor = -1
	log = []*pb.LogEntry{{Index: 0, Term: 0}}

	if v, e := p.db.Get(metaTermKey, nil); e == nil {
		term = decodeInt64(v)
	} else if e != leveldb.ErrNotFound {
		err = e
		return
	}
	if v, e := p.db.Get(metaVotedKey, nil); e == nil {
		votedFor = decodeInt32(v)
	} else if e != leveldb.ErrNotFound {
		err = e
		return
	}
	if v, e := p.db.Get(metaCommitKey, nil); e == nil {
		commit = decodeInt64(v)
	} else if e != leveldb.ErrNotFound {
		err = e
		return
	}

	iter := p.db.NewIterator(util.BytesPrefix(logPrefix), nil)
	defer iter.Release()
	for iter.Next() {
		entry := &pb.LogEntry{}
		if err = proto.Unmarshal(iter.Value(), entry); err != nil {
			return
		}
		log = append(log, entry)
	}
	if iterErr := iter.Error(); iterErr != nil {
		err = iterErr
	}
	return
}
