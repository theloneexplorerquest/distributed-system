package shardctrler

import (
	"6.5840/raft"
	"log"
	"sort"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu          sync.Mutex
	me          int
	rf          *raft.Raft
	applyCh     chan raft.ApplyMsg
	channels    map[int64]chan Op
	prevRequest map[int64]map[int64]struct{}
	term        int
	// Your data here.

	configs []Config // indexed by config num
}

const (
	JOIN  string = "JOIN"
	LEAVE        = "LEAVE"
	MOVE         = "MOVE"
	QUERY        = "QUERY"
)

type Op struct {
	Operation string
	Args      interface{}
	ClientId  int64
	SeqNum    int64
	Term      int
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

func (sc *ShardCtrler) LogCurrentShardAssignment() {
	//sc.mu.Lock()
	//defer sc.mu.Unlock()

	// Get the shard assignment of the latest configuration
	//lastConfig := sc.configs[len(sc.configs)-1]
	//shardAssignment := lastConfig.Shards

	// Log the current shard assignment
	//log.Printf("idx %d Current Shard Assignment:", lastConfig.Num)
	//for shard, gid := range shardAssignment {
	//	log.Printf("Shard %d: Group %d\n", shard, gid)
	//}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	operation := Op{Operation: JOIN, Args: *args, ClientId: args.ClientId, SeqNum: args.SeqNum}
	_, _, isLeader := sc.rf.Start(operation)
	//log.Printf(" start join command s%d %v", sc.me, args.Servers)

	if !isLeader {
		reply.WrongLeader = true
		return
	}
	opChan := make(chan Op, 1)
	sc.mu.Lock()
	sc.channels[args.ClientId^args.SeqNum] = opChan
	sc.mu.Unlock()
	for {
		select {
		case <-opChan:
			sc.mu.Lock()
			delete(sc.channels, args.ClientId^args.SeqNum)
			reply.WrongLeader = false
			sc.mu.Unlock()
			return
		default:
			_, stillLeader := sc.rf.GetState()
			if !stillLeader {
				sc.mu.Lock()
				reply.WrongLeader = true
				delete(sc.channels, args.ClientId^args.SeqNum)
				sc.mu.Unlock()
				return
			}
			time.Sleep(2 * time.Millisecond) // Sleep for a short while to avoid busy waiting
		}
	}
}

// Rebalance the shards among the groups
func (sc *ShardCtrler) rebalanceShards(config *Config) {
	numGroups := len(config.Groups)
	if numGroups == 0 {
		// No groups to assign shards to
		for i := range config.Shards {
			config.Shards[i] = 0
		}
		return
	}

	// Create a list of GIDs
	gids := make([]int, 0, numGroups)
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	// Calculate the desired number of shards per group
	desiredShardsPerGroup := NShards / numGroups
	extraShards := NShards % numGroups

	// Initialize shard counts
	shardCounts := make(map[int]int)
	for _, gid := range gids {
		shardCounts[gid] = 0
	}

	// First pass: count current shard assignments and collect unassigned shards
	unassignedShards := []int{}
	for i, gid := range config.Shards {
		if gid != 0 {
			if _, exists := shardCounts[gid]; exists {
				shardCounts[gid]++
			} else {
				unassignedShards = append(unassignedShards, i)
			}
		} else {
			unassignedShards = append(unassignedShards, i)
		}
	}

	log.Printf("unassign1 %d", config.Shards)

	// Adjust the assignment to ensure each group gets roughly the same number of shards
	for i := range config.Shards {
		gid := config.Shards[i]
		if gid != 0 {
			if shardCounts[gid] > desiredShardsPerGroup || (shardCounts[gid] == desiredShardsPerGroup && extraShards == 0) {
				unassignedShards = append(unassignedShards, i)
				shardCounts[gid]--
				config.Shards[i] = 0
			} else if shardCounts[gid] == desiredShardsPerGroup {
				extraShards--
			}
		}
	}

	log.Printf("unassign2 %d", config.Shards)
	// Assign unassigned shards
	for _, shard := range unassignedShards {
		for i := range gids {
			gid := gids[i]
			if shardCounts[gid] < desiredShardsPerGroup || (shardCounts[gid] < desiredShardsPerGroup+1 && extraShards > 0) {
				config.Shards[shard] = gid
				shardCounts[gid]++
				if shardCounts[gid] == desiredShardsPerGroup+1 {
					extraShards--
				}
				break
			}
		}
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {

	operation := Op{Operation: LEAVE, Args: *args, ClientId: args.ClientId, SeqNum: args.SeqNum}
	//log.Printf("start leave command s%d %v", sc.me, args.GIDs)

	_, _, isLeader := sc.rf.Start(operation)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	opChan := make(chan Op, 1)
	sc.mu.Lock()
	sc.channels[args.ClientId^args.SeqNum] = opChan
	//log.Printf("mid leave command s%d %v", sc.me, args.GIDs)
	sc.mu.Unlock()
	for {
		select {
		case <-opChan:
			sc.mu.Lock()
			delete(sc.channels, args.ClientId^args.SeqNum)
			reply.WrongLeader = false
			sc.mu.Unlock()
			return
		default:
			_, stillLeader := sc.rf.GetState()
			if !stillLeader {
				sc.mu.Lock()
				reply.WrongLeader = true
				delete(sc.channels, args.ClientId^args.SeqNum)
				sc.mu.Unlock()
				return
			}
			time.Sleep(2 * time.Millisecond) // Sleep for a short while to avoid busy waiting
		}
	}
}

func contains(slice []int, element int) bool {
	for _, e := range slice {
		if e == element {
			return true
		}
	}
	return false
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	operation := Op{Operation: MOVE, Args: *args, ClientId: args.ClientId, SeqNum: args.SeqNum}
	_, _, isLeader := sc.rf.Start(operation)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	opChan := make(chan Op, 1)
	sc.mu.Lock()
	sc.channels[args.ClientId^args.SeqNum] = opChan
	sc.mu.Unlock()
	for {
		select {
		case <-opChan:
			sc.mu.Lock()
			delete(sc.channels, args.ClientId^args.SeqNum)
			reply.WrongLeader = false
			sc.mu.Unlock()
			return
		default:
			_, stillLeader := sc.rf.GetState()
			if !stillLeader {
				sc.mu.Lock()
				reply.WrongLeader = true
				delete(sc.channels, args.ClientId^args.SeqNum)
				sc.mu.Unlock()
				return
			}
			time.Sleep(2 * time.Millisecond) // Sleep for a short while to avoid busy waiting
		}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	operation := Op{Operation: QUERY, Args: *args, ClientId: args.ClientId, SeqNum: args.SeqNum}
	_, _, isLeader := sc.rf.Start(operation)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	opChan := make(chan Op, 1)
	sc.mu.Lock()
	sc.channels[args.ClientId^args.SeqNum] = opChan
	//log.Printf("return query")
	sc.mu.Unlock()
	for {
		select {
		case result := <-opChan:
			sc.mu.Lock()
			//log.Printf("return query s%d", sc.me)
			delete(sc.channels, args.ClientId^args.SeqNum)
			reply.WrongLeader = false
			num := result.Args.(QueryArgs).Num
			if num == -1 || num >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
				//log.Printf("s%d c%d query n%d g%d b%t", sc.me, args.ClientId, reply.Config.Num, len(reply.Config.Groups), reply.WrongLeader)
				sc.mu.Unlock()
				return
			}
			for _, config := range sc.configs {
				if config.Num == num {
					reply.Config = config
					//log.Printf("s%d c%d query n%d g%d b%t", sc.me, args.ClientId, reply.Config.Num, len(reply.Config.Groups), reply.WrongLeader)
					sc.mu.Unlock()
					return
				}
			}
			//log.Printf("s%d c%d query n%d g%d b%t", sc.me, args.ClientId, reply.Config.Num, len(reply.Config.Groups), reply.WrongLeader)
			sc.mu.Unlock()
			return
		default:
			_, stillLeader := sc.rf.GetState()
			if !stillLeader {
				sc.mu.Lock()
				reply.WrongLeader = true
				delete(sc.channels, args.ClientId^args.SeqNum)
				sc.mu.Unlock()
				return
			}
			time.Sleep(2 * time.Millisecond) // Sleep for a short while to avoid busy waiting
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Num = 0

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.channels = make(map[int64]chan Op)
	sc.prevRequest = make(map[int64]map[int64]struct{})
	sc.term = -1
	go sc.runSCServer()
	// Your code here.

	return sc
}

func (sc *ShardCtrler) runSCServer() {
	for !sc.rf.Killed() {
		select {
		case applyMsg := <-sc.applyCh:
			if !applyMsg.CommandValid {
				continue
			}
			command := applyMsg.Command.(Op)
			sc.mu.Lock()
			if _, exists := sc.prevRequest[command.ClientId][command.SeqNum]; exists {
				// Duplicate request, ignore
				//log.Printf("s%d dul %s %s", kv.me, command.Key, command.Value)

				if ch, ok := sc.channels[command.ClientId^command.SeqNum]; ok {
					ch <- command
				}
				sc.mu.Unlock()
				continue
			}
			if _, exists := sc.prevRequest[command.ClientId]; !exists {
				// If not, create a new set
				sc.prevRequest[command.ClientId] = make(map[int64]struct{})
			}
			// Add the element to the set
			sc.prevRequest[command.ClientId][command.SeqNum] = struct{}{}
			//log.Printf("command s%d o%s", sc.me, command.Operation)
			switch command.Operation {
			case JOIN:
				//log.Printf("join command s%d %v", sc.me, command.Args.(JoinArgs).Servers)
				lastConfig := sc.configs[len(sc.configs)-1]
				newConfig := Config{
					Num:    len(sc.configs),
					Shards: lastConfig.Shards,
					Groups: make(map[int][]string),
				}
				for gid, servers := range lastConfig.Groups {
					newConfig.Groups[gid] = servers
				}
				for gid, servers := range command.Args.(JoinArgs).Servers {
					//log.Printf("group %d: server %s\n", gid, servers)
					newConfig.Groups[gid] = servers
				}
				sc.rebalanceShards(&newConfig)
				sc.configs = append(sc.configs, newConfig)
			case LEAVE:
				//log.Printf("leave command s%d %v", sc.me, command.Args.(LeaveArgs).GIDs)
				lastConfig := sc.configs[len(sc.configs)-1]
				newConfig := Config{
					Num:    len(sc.configs),
					Shards: lastConfig.Shards,
					Groups: make(map[int][]string),
				}
				for gid, servers := range lastConfig.Groups {
					if !contains(command.Args.(LeaveArgs).GIDs, gid) {
						newConfig.Groups[gid] = servers
					}
				}
				sc.rebalanceShards(&newConfig)

				sc.configs = append(sc.configs, newConfig)
			case MOVE:
				//log.Printf("move command")
				lastConfig := sc.configs[len(sc.configs)-1]
				newConfig := Config{
					Num:    len(sc.configs),
					Shards: lastConfig.Shards,
					Groups: lastConfig.Groups,
				}
				newConfig.Shards[command.Args.(MoveArgs).Shard] = command.Args.(MoveArgs).GID
				sc.configs = append(sc.configs, newConfig)
			case QUERY:
				//log.Printf("query command s%d", sc.me)
			}
			if ch, ok := sc.channels[command.ClientId^command.SeqNum]; ok {
				ch <- command
				close(ch)
				delete(sc.channels, command.ClientId^command.SeqNum)
			}
			sc.mu.Unlock()
		}
	}
}
