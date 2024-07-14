package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
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
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	getCh    chan bool
	putCh    chan bool
	appendCh chan bool
	dead     int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	m map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	operation := Op{Operation: GET, Key: args.Key}
	//kv.rf.Start(operation)
	_, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	} else {
		//for {
		//	select {
		//	case <-kv.getCh:
		//		value, exists := kv.m[args.Key]
		//		if exists {
		//			reply.Value = value
		//		} else {
		//			reply.Value = ""
		//		}
		//		return
		//	}
		//}
	}
	//value, exists := kv.m[args.Key]
	//if exists {
	//	reply.Value = value
	//} else {
	//	reply.Value = ""
	//}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//log.Printf("start put")
	operation := Op{Operation: PUT, Key: args.Key, Value: args.Value}
	_, _, isLeader := kv.rf.Start(operation)
	//log.Printf("after start")
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	} else {
		for {
			select {
			case <-kv.putCh:
				log.Printf("return put")
				kv.m[args.Key] = args.Value
				kv.putCh = make(chan bool)
				return
			}
		}
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	operation := Op{Operation: APPEND, Key: args.Key, Value: args.Value}
	_, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	} else {
		//for {
		//	select {
		//	case <-kv.appendCh:
		//		if value, keyExists := kv.m[args.Key]; keyExists {
		//			kv.m[args.Key] = value + args.Value
		//		} else {
		//			kv.m[args.Key] = args.Value
		//		}
		//		return
		//	}
		//}
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
	//log.Printf("get applyCh: %v\n", kv.applyCh)
	// You may need initialization code here.
	go kv.runKVServer()
	return kv
}

func (kv *KVServer) runKVServer() {
	for !kv.killed() {
		select {
		case applyCh := <-kv.applyCh:
			log.Printf("applyCh a %v %t \n", applyCh.Command, applyCh.CommandValid)
			command := applyCh.Command.(Op)
			if command.Operation == PUT {
				kv.putCh <- true
			}
			//} else if command.Operation == APPEND {
			//	kv.appendCh <- true
			//} else if command.Operation == GET {
			//	kv.getCh <- true
			//}
		}
		log.Printf("runServer")
	}
}
func (kv *KVServer) resetChannels() {
	kv.getCh = make(chan bool)
	kv.putCh = make(chan bool)
	kv.appendCh = make(chan bool)
}
