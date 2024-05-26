package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu               sync.Mutex
	m                map[string]string
	lastRequest      map[string]int64
	lastRequestValue map[string]string
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exists := kv.m[args.Key]
	if exists {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.SeqNum == kv.lastRequest[args.ClientId] {
		return
	}
	kv.lastRequest[args.ClientId] = args.SeqNum
	//kv.lastRequestV[args.ClientId] = args.SeqNum
	kv.m[args.Key] = args.Value
	return
	// Your code here.
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.SeqNum == kv.lastRequest[args.ClientId] {
		reply.Value = kv.lastRequestValue[args.ClientId]
		return
	}
	if value, keyExists := kv.m[args.Key]; keyExists {
		reply.Value = value
		kv.m[args.Key] = value + args.Value
	} else {
		reply.Value = ""
		kv.m[args.Key] = args.Value
	}
	kv.lastRequest[args.ClientId] = args.SeqNum
	kv.lastRequestValue[args.ClientId] = reply.Value
	return
}

//func (kv *KVServer) StartCleanupRoutine() {
//	ticker := time.NewTicker(5 * time.Second) // Adjust the frequency as necessary
//	go func() {
//		for range ticker.C {
//			kv.CleanupOldEntries()
//		}
//	}()
//}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.m = make(map[string]string)
	kv.lastRequest = make(map[string]int64)
	kv.lastRequestValue = make(map[string]string)
	// You may need initialization code here.
	//kv.StartCleanupRoutine()
	return kv
}
