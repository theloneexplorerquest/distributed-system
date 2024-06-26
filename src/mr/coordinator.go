package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type MapTask struct {
	filename string
	index    int
}

const (
	NotStarted = iota
	Started
	Finished
)

var maptasks chan MapTask

type Coordinator struct {
	// Your definitions here.
	mapTaskStatus     map[string]int
	reduceTaskStatus  map[int]int
	finish            bool
	inputFiles        []string
	nReduce           int
	mapIndex          int
	reduceIndex       int
	intermediateFiles [][]string
	RWMutexLock       *sync.RWMutex
	mapFinished       bool
	reduceFinished    bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.RWMutexLock.Lock()
	defer c.RWMutexLock.Unlock()

	// Your code here.

	return c.mapFinished
}

func (c *Coordinator) DistributeTask(args *MrArgs, reply *MrReply) error {
	select {
	case mapTask := <-maptasks:
		reply.MapFileName = mapTask.filename
		reply.Index = mapTask.index
		reply.TaskType = "map"
		reply.NReduce = c.nReduce
		c.RWMutexLock.Lock()
		c.mapTaskStatus[mapTask.filename] = Started
		c.RWMutexLock.Unlock()
		go c.watchWorkerMap(mapTask)
		return nil
	case reduceNumber := <-reducetasks:
		reply.Files = c.intermediateFiles[reduceNumber]
		reply.Index = reduceNumber
		reply.TaskType = "reduce"
		c.RWMutexLock.Lock()
		c.reduceTaskStatus[reduceNumber] = Started
		c.RWMutexLock.Unlock()
		go c.watchWorkerReduce(reduceNumber)
		return nil
	}
	return nil
}

func (c *Coordinator) watchWorkerReduce(reducedNumber int) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.RWMutexLock.Lock()
			c.reduceTaskStatus[reducedNumber] = NotStarted
			c.RWMutexLock.Unlock()
			reducetasks <- reducedNumber
		default:
			c.RWMutexLock.RLock()
			if c.reduceTaskStatus[reducedNumber] == Finished {
				c.RWMutexLock.RUnlock()
				return
			}
			c.RWMutexLock.RUnlock()
		}
	}
}

func (c *Coordinator) watchWorkerMap(task MapTask) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.RWMutexLock.Lock()
			c.mapTaskStatus[task.filename] = NotStarted
			c.RWMutexLock.Unlock()
			maptasks <- task
		default:
			c.RWMutexLock.RLock()
			if c.mapTaskStatus[task.filename] == Finished {
				c.RWMutexLock.RUnlock()
				return
			}
			c.RWMutexLock.RUnlock()
		}
	}
}

func (c *Coordinator) NotifyIntermediateFile(args *NotifyIntermediateArgs, reply *NotifyReply) error {
	c.RWMutexLock.Lock()
	defer c.RWMutexLock.Unlock()
	c.intermediateFiles[args.ReducedIndex] = append(c.intermediateFiles[args.ReducedIndex], args.File)
	return nil
}

func (c *Coordinator) NotifyMapSuccess(args *NotifyMapSuccessArgs, reply *NotifyReply) error {
	c.RWMutexLock.Lock()
	defer c.RWMutexLock.Unlock()
	c.mapTaskStatus[args.File] = Finished
	finished := true
	for _, v := range c.mapTaskStatus {
		if v != Finished {
			finished = false
			break
		}
	}
	c.mapFinished = finished
	if c.mapFinished {
		for i := 0; i < c.nReduce; i++ {
			c.reduceTaskStatus[i] = NotStarted
			reducetasks <- i
		}
	}
	return nil
}

func (c *Coordinator) NotifyReduceSuccess(args *NotifyReduceSuccessArgs, reply *NotifyReply) error {
	c.RWMutexLock.Lock()
	defer c.RWMutexLock.Unlock()
	c.reduceTaskStatus[args.ReducedIndex] = Finished
	finished := true
	for _, v := range c.reduceTaskStatus {
		if v != Finished {
			finished = false
			break
		}
	}
	c.reduceFinished = finished
	return nil
}

var reducetasks chan int

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	maptasks = make(chan MapTask, len(files))
	reducetasks = make(chan int, nReduce)
	c.mapTaskStatus = make(map[string]int, len(files))
	c.reduceTaskStatus = make(map[int]int, nReduce)
	for index, file := range files {
		c.mapTaskStatus[file] = NotStarted
		mapTask := MapTask{filename: file, index: index}
		maptasks <- mapTask
	}
	c.inputFiles = files
	c.nReduce = nReduce
	c.intermediateFiles = make([][]string, nReduce)
	c.RWMutexLock = new(sync.RWMutex)
	c.server()
	return &c
}
