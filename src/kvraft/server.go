package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type	string
	Key 	string
	Value 	string
	Cid 	int64
	Seq		int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	store	map[string]string   // key-value store
	request map[int64]int		// client cid to seq map for deduplication
	result	map[int]chan Op		// log index to Type chan map for checking if request succeeds
}


func (kv *RaftKV) sendOpToLog(op Op) bool {
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.result[index]

	if !ok {
		ch = make(chan Op, 1)
		kv.result[index] = ch
	}

	kv.mu.Unlock()

	select {
		case cmd := <-ch:
			return cmd == op
		case <-time.After(800 * time.Millisecond):
			return false
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Type: "Get", Key: args.Key}
	ok := kv.sendOpToLog(op)

	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	kv.mu.Lock()
	val, has := kv.store[args.Key]
	kv.mu.Unlock()

	if has {
		reply.Err = OK
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Type: args.Type, Key: args.Key,
			 Value: args.Value, Cid: args.Cid, Seq: args.Seq}
	ok := kv.sendOpToLog(op)

	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.Err = OK
}

func (kv *RaftKV) saveSnapshot() {

}

func (kv *RaftKV) readSnapshot(data []byte) {

}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *RaftKV) execute(op Op) {
	switch op.Type {
	case "Put":
		kv.store[op.Key] = op.Value
	case "Append":
		kv.store[op.Key] += op.Value
	}
}

func (kv *RaftKV) apply() {
	for {
		msg := <-kv.applyCh
		op := msg.Command.(Op)

		kv.mu.Lock()

		if op.Type != "Get" {
			if seq, ok := kv.request[op.Cid]; !ok || op.Seq > seq {
				kv.execute(op)
				kv.request[op.Cid] = op.Seq
			}
		}

		ch, ok := kv.result[msg.Index]

		if ok {
			ch <- op
		}

		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.store = make(map[string]string)
	kv.request = make(map[int64]int)
	kv.result = make(map[int]chan Op)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	go kv.apply()

	return kv
}
