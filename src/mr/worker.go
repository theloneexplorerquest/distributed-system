package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for true {
		reply := CallCoordinator()
		if reply.TaskType == "map" {
			executeMap(reply, mapf)
		} else if reply.TaskType == "reduce" {
			executeReduce(reply, reducef)
		} else {
			break
		}
	}
}

func executeMap(reply MrReply, mapf func(string, string) []KeyValue) {
	fileName := reply.MapFileName
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	kvap := ArrangeIntermediate(kva, reply.NReduce)
	files := []string{}
	for i := range kvap {
		values := kvap[i]
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(filename)
		enc := json.NewEncoder(ofile)
		for _, kv := range values {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("error: ", err)
			}
		}
		files = append(files, filename)
		NotifyCoorindator(i, filename)
		ofile.Close()
	}
	NotifyMapSuccess(fileName)
}

func NotifyCoorindator(reducedIndex int, filename string) {
	args := NotifyIntermediateArgs{}
	args.ReducedIndex = reducedIndex
	args.File = filename
	reply := NotifyReply{}
	call("Coordinator.NotifyIntermediateFile", &args, &reply)
}

func NotifyMapSuccess(filename string) {
	args := NotifyMapSuccessArgs{}
	args.File = filename
	reply := NotifyReply{}
	call("Coordinator.NotifyMapSuccess", &args, &reply)
}

func NotifyReduceSuccess(reduceIndex int) {
	args := NotifyReduceSuccessArgs{}
	args.ReducedIndex = reduceIndex
	reply := NotifyReply{}
	call("Coordinator.NotifyReduceSuccess", &args, &reply)
}

func ArrangeIntermediate(kva []KeyValue, nReduce int) [][]KeyValue {
	kvap := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		v := ihash(kv.Key) % nReduce
		kvap[v] = append(kvap[v], kv)
	}
	return kvap
}

func executeReduce(reply MrReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for _, v := range reply.Files {
		file, err := os.Open(v)
		if err != nil {
			log.Fatalf("cannot open %v", v)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out" + strconv.Itoa(reply.Index)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	NotifyReduceSuccess(reply.Index)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func CallCoordinator() MrReply {
	arg := MrArgs{}
	reply := MrReply{}
	call("Coordinator.DistributeTask", &arg, &reply)
	return reply
}
