package kvraft

import (
	"6.5840/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers    []*labrpc.ClientEnd
	n          int64
	prevLeader int64
	clientId   int64
	seqNum     int64
	mu         sync.Mutex
	// You will have to modify this struct.
}

func (ck *Clerk) nextSeqNum() {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.seqNum++
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.n = int64(len(servers))
	ck.prevLeader = -1
	ck.seqNum = 0
	ck.clientId = nrand()
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	//log.Printf("client get")
	for {
		args := GetArgs{}
		args.Key = key
		args.ClientId = ck.clientId
		args.SeqNum = ck.seqNum
		reply := GetReply{}
		serverId := nrand() % ck.n
		if ck.prevLeader != -1 {
			//log.Printf("know server")
			serverId = ck.prevLeader
		}
		response := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
		if response && reply.Err != ErrWrongLeader {
			ck.prevLeader = serverId
			ck.nextSeqNum()
			return reply.Value
		} else {
			//log.Printf("s%d is wrong leader from c%d\n", serverId, ck.clientId)
			ck.prevLeader = -1
		}
		time.Sleep(10 * time.Nanosecond)
		//log.Printf("retry")
	}
	// You will have to modify this function.
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	for {
		//log.Printf("timeoutPutStart")
		response := false
		args := PutAppendArgs{}
		args.Key = key
		args.Value = value
		args.ClientId = ck.clientId
		args.SeqNum = ck.seqNum
		reply := PutAppendReply{}
		serverId := nrand() % ck.n
		if ck.prevLeader != -1 {
			//log.Printf("know server")
			serverId = ck.prevLeader
		}
		if op == "Put" {
			response = ck.servers[serverId].Call("KVServer.Put", &args, &reply)
		} else {
			response = ck.servers[serverId].Call("KVServer.Append", &args, &reply)
		}
		//log.Printf("timeoutPutStart %t Err %s", response, reply.Err)
		if response && reply.Err != ErrWrongLeader {
			//log.Printf("s%d is a leader from c%d\n for k %s,v %s ", serverId, ck.clientId, args.Key, args.Value)
			ck.nextSeqNum()
			ck.prevLeader = serverId
			return
		} else {
			//log.Printf("s%d is wrong leader from c%d\n", serverId, ck.clientId)
			ck.prevLeader = -1
		}
		time.Sleep(10 * time.Nanosecond)
	}
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
