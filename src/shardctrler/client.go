package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.5840/labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	n          int64
	prevLeader int64
	clientId   int64
	seqNum     int64
	mu         sync.Mutex
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
	// Your code here.
	ck.prevLeader = -1
	ck.seqNum = 0
	ck.clientId = nrand()
	ck.n = int64(len(servers))
	return ck
}

func (ck *Clerk) Query(num int) Config {
	for {
		args := QueryArgs{}
		// Your code here.
		args.Num = num
		args.ClientId = ck.clientId
		args.SeqNum = ck.seqNum
		reply := QueryReply{}
		serverId := nrand() % ck.n
		if ck.prevLeader != -1 {
			//log.Printf("know server")
			serverId = ck.prevLeader
		}
		//log.Printf("s%d c%d Config return g%d", serverId, ck.clientId, len(reply.Config.Groups))

		response := ck.servers[serverId].Call("ShardCtrler.Query", &args, &reply)
		//log.Printf("s%d c%d Config return g%d b%t", serverId, ck.clientId, len(reply.Config.Groups), reply.WrongLeader)
		if response && !reply.WrongLeader {
			ck.prevLeader = serverId
			ck.nextSeqNum()
			//log.Printf("s%d c%d Config return g%d", serverId, ck.clientId, len(reply.Config.Groups))
			return reply.Config
		} else {
			ck.prevLeader = -1
		}
		time.Sleep(10 * time.Nanosecond)
		//log.Printf("-------------------------------------------")
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	for {
		args := JoinArgs{}
		args.Servers = servers
		args.ClientId = ck.clientId
		args.SeqNum = ck.seqNum
		var reply JoinReply
		serverId := nrand() % ck.n
		if ck.prevLeader != -1 {
			//log.Printf("know server")
			serverId = ck.prevLeader
		}
		ok := ck.servers[serverId].Call("ShardCtrler.Join", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.prevLeader = serverId
			ck.nextSeqNum()
			return
		} else {
			ck.prevLeader = -1
		}
		time.Sleep(10 * time.Nanosecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	for {
		args := LeaveArgs{}
		args.GIDs = gids
		args.ClientId = ck.clientId
		args.SeqNum = ck.seqNum
		var reply LeaveReply
		serverId := nrand() % ck.n
		if ck.prevLeader != -1 {
			//log.Printf("know server")
			serverId = ck.prevLeader
		}
		ok := ck.servers[serverId].Call("ShardCtrler.Leave", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.prevLeader = serverId
			ck.nextSeqNum()
			return
		} else {
			ck.prevLeader = -1
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	for {
		args := MoveArgs{}
		args.GID = gid
		args.Shard = shard
		args.ClientId = ck.clientId
		args.SeqNum = ck.seqNum
		var reply MoveReply
		serverId := nrand() % ck.n
		if ck.prevLeader != -1 {
			serverId = ck.prevLeader
		}
		ok := ck.servers[serverId].Call("ShardCtrler.Move", &args, &reply)
		if ok && reply.WrongLeader == false {
			ck.prevLeader = serverId
			ck.nextSeqNum()
			return
		} else {
			ck.prevLeader = -1
		}
	}
}
