package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	_ "net/http/pprof"
	"sync"
	"sync/atomic"
	"time"
)

//const Debug = false
//
//func DPrintf(format string, a ...interface{}) (n int, err error) {
//	if Debug {
//		log.Printf(format, a...)
//	}
//	return
//}

const (
	GET    string = "GET"
	PUT           = "PUT"
	APPEND        = "APPEND"
)

type Op struct {
	Operation string
	Key       string
	Value     string
	ClientId  int64
	SeqNum    int64
	Term      int
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu          sync.Mutex
	me          int
	rf          *raft.Raft
	applyCh     chan raft.ApplyMsg
	getCh       chan bool
	putCh       chan bool
	appendCh    chan bool
	dead        int32 // set by Kill()
	channels    map[int64]chan Op
	prevRequest map[int64]map[int64]struct{}
	term        int

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	m map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	operation := Op{Operation: GET, Key: args.Key, ClientId: args.ClientId, SeqNum: args.SeqNum}
	_, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	opChan := make(chan Op, 1)
	kv.mu.Lock()
	kv.channels[args.ClientId^args.SeqNum] = opChan
	kv.mu.Unlock()

	for {
		select {
		case result := <-opChan:
			kv.mu.Lock()
			delete(kv.channels, args.ClientId^args.SeqNum)
			if result.Operation == GET {
				reply.Value = kv.m[args.Key]
				reply.Err = OK
			} else {
				reply.Err = ErrWrongLeader
			}
			kv.mu.Unlock()
			return
		default:
			_, stillLeader := kv.rf.GetState()
			if !stillLeader {
				reply.Err = ErrWrongLeader
				kv.mu.Lock()
				delete(kv.channels, args.ClientId^args.SeqNum)
				kv.mu.Unlock()
				return
			}
			time.Sleep(2 * time.Millisecond) // Sleep for a short while to avoid busy waiting
		}
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if _, exists := kv.prevRequest[args.ClientId][args.SeqNum]; exists {
		//log.Printf("caught in put")
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	operation := Op{Operation: PUT, Key: args.Key, Value: args.Value, ClientId: args.ClientId, SeqNum: args.SeqNum}
	_, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//log.Printf("s%d put %s:%s from c%d seq%d idx%d ", kv.me, args.Key, args.Value, args.ClientId, args.SeqNum, index)
	opChan := make(chan Op, 1)
	kv.mu.Lock()
	kv.channels[args.ClientId^args.SeqNum] = opChan
	kv.mu.Unlock()
	for {
		select {
		case <-opChan:
			kv.mu.Lock()
			delete(kv.channels, args.ClientId^args.SeqNum)
			kv.mu.Unlock()
			return
		default:
			// Continuously check if the leadership is still valid
			_, stillLeader := kv.rf.GetState()
			if !stillLeader {
				reply.Err = ErrWrongLeader
				kv.mu.Lock()
				delete(kv.channels, args.ClientId^args.SeqNum)
				kv.mu.Unlock()
				return
			}
			time.Sleep(2 * time.Millisecond) // Sleep for a short while to avoid busy waiting
		}
	}

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if _, exists := kv.prevRequest[args.ClientId][args.SeqNum]; exists {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	operation := Op{Operation: APPEND, Key: args.Key, Value: args.Value, ClientId: args.ClientId, SeqNum: args.SeqNum}
	_, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	opChan := make(chan Op, 1)
	kv.mu.Lock()
	kv.channels[args.ClientId^args.SeqNum] = opChan
	kv.mu.Unlock()
	for {
		select {
		case <-opChan:
			kv.mu.Lock()
			delete(kv.channels, args.ClientId^args.SeqNum)
			kv.mu.Unlock()
			return
		default:
			_, stillLeader := kv.rf.GetState()
			if !stillLeader {
				reply.Err = ErrWrongLeader
				kv.mu.Lock()
				delete(kv.channels, args.ClientId^args.SeqNum)
				kv.mu.Unlock()
				return
			}
			time.Sleep(2 * time.Millisecond) // Sleep for a short while to avoid busy waiting
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.getCh = make(chan bool)
	kv.putCh = make(chan bool)
	kv.appendCh = make(chan bool)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.m = make(map[string]string)
	kv.channels = make(map[int64]chan Op)
	kv.prevRequest = make(map[int64]map[int64]struct{})
	kv.term = -1
	//log.Printf("get applyCh: %v\n", kv.applyCh)
	// You may need initialization code here.
	go kv.runKVServer()
	return kv
}

func (kv *KVServer) runKVServer() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			if !applyMsg.CommandValid {
				continue
			}
			command := applyMsg.Command.(Op)
			kv.mu.Lock()
			if _, exists := kv.prevRequest[command.ClientId][command.SeqNum]; exists {
				// Duplicate request, ignore
				//log.Printf("s%d dul %s %s", kv.me, command.Key, command.Value)

				if ch, ok := kv.channels[command.ClientId^command.SeqNum]; ok {
					ch <- command
				}
				kv.mu.Unlock()
				continue
			}
			if _, exists := kv.prevRequest[command.ClientId]; !exists {
				// If not, create a new set
				kv.prevRequest[command.ClientId] = make(map[int64]struct{})
			}
			// Add the element to the set
			kv.prevRequest[command.ClientId][command.SeqNum] = struct{}{}

			switch command.Operation {
			case PUT:
				//log.Printf("s%d emit %s:%s for c%d seq%d idx%d", kv.me, command.Key, command.Value, command.ClientId, command.SeqNum, applyMsg.CommandIndex)
				kv.m[command.Key] = command.Value
			case APPEND:
				//log.Printf("s%d emit %s:%s for c%d seq%d idx%d", kv.me, command.Key, command.Value, command.ClientId, command.SeqNum, applyMsg.CommandIndex)
				kv.m[command.Key] += command.Value
			case GET:
				// do nothing
			}
			if ch, ok := kv.channels[command.ClientId^command.SeqNum]; ok {
				ch <- command
				close(ch)
				delete(kv.channels, command.ClientId^command.SeqNum)
			}
			kv.mu.Unlock()
		}
	}
}
