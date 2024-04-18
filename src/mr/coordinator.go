package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	NodeIdle    = 0
	NodeRunning = 1
	NodeFailed  = 2

	JobMapStage    = 0
	JobReduceStage = 1
	JobDoneStage   = 2

	TaskMapType    = 0
	TaskReduceType = 1
	TaskNAType     = 2
	TaskAllDonType = 3

	TaskWait     = 0
	TaskRunning  = 1
	TaskFinished = 2

	OvertimeLimit = 10
)

type Node struct {
	ID    int
	State int
}

type Task struct {
	ID         int
	NID        int
	Type       int
	State      int
	SentAt     int64
	FinishedAt int64
}

type Coordinator struct {
	// Your definitions here.
	Mutex         sync.Mutex
	Stage         int
	NReduce       int
	MapFinCnt     int
	ReduceFinCnt  int
	Nodes         map[int]*Node
	Tasks         map[int]map[int]*Task
	MapFiles      map[int]string
	Intermediates map[int]map[int]string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if _, ok := c.Nodes[args.UID]; !ok {
		c.Nodes[args.UID] = &Node{
			ID:    args.UID,
			State: NodeIdle,
		}
	}

	reply.NID = args.UID
	reply.NReduce = c.NReduce
	return nil
}

func (c *Coordinator) Complete(args *CompleteArgs, reply *CompleteReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	task, ok := c.Tasks[args.Type][args.TID]
	if !ok {
		log.Println("Task not found", args)
		return errors.New("Task not found")
	}

	node, ok := c.Nodes[args.NID]
	if !ok {
		log.Println("Node not found", args)
		return errors.New("Node not found")
	}
	node.State = NodeIdle

	if task.State == TaskFinished {
		log.Println("Task already done", task)
		return nil
	}

	if args.Type == TaskMapType {
		err := c.updateIntermediates(args.Files)
		if err != nil {
			return errors.New("updateIntermediates failed")
		}

		c.MapFinCnt++
		if c.MapFinCnt == len(c.Tasks[TaskMapType]) {
			c.Stage = JobReduceStage
		}
	} else {
		c.ReduceFinCnt++
	}
	task.State = TaskFinished
	task.FinishedAt = time.Now().Unix()
	return nil
}

func (c *Coordinator) Next(args *NextArgs, reply *NextReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	node, ok := c.Nodes[args.NID]
	if !ok {
		log.Println("Node not found", args)
		return errors.New("Node not found")
	}
	if node.State == NodeRunning {
		log.Println("Node busy", node)
		return errors.New("Node busy")
	}
	if node.State == NodeFailed {
		log.Println("Node failed", node)
		return errors.New("Node failed")
	}

	if c.Stage == JobDoneStage {
		reply.TID = -1
		reply.Type = TaskAllDonType
		return nil
	}

	taskType := TaskMapType
	if c.Stage == JobReduceStage {
		taskType = TaskReduceType
	}
	for _, task := range c.Tasks[taskType] {
		if task.State != TaskWait {
			continue
		}

		task.NID = args.NID
		task.State = TaskRunning
		task.SentAt = time.Now().Unix()

		node.State = NodeRunning

		reply.TID = task.ID
		reply.Type = taskType
		reply.Files = c.fetchTaskFiles(task)
		return nil
	}

	reply.TID = -1
	reply.Type = TaskNAType
	return nil
}

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
	// Your code here.
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	res := c.ReduceFinCnt == len(c.Tasks[TaskReduceType])
	if res {
		c.Stage = JobDoneStage
	}
	return res
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Tasks = make(map[int]map[int]*Task)
	c.Intermediates = make(map[int]map[int]string)
	c.Nodes = make(map[int]*Node)
	c.MapFiles = make(map[int]string)
	c.NReduce = nReduce

	c.initializeTasks(files, nReduce)
	c.initializeIntermediates(nReduce)

	c.server()

	go func() {
		for {
			c.CheckOvertimeTasks()
			time.Sleep(500 * time.Millisecond)
		}
	}()

	return &c
}

func (c *Coordinator) CheckOvertimeTasks() {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	taskType := TaskMapType
	if c.Stage == JobReduceStage {
		taskType = TaskReduceType
	}

	now := time.Now().Unix()
	for _, task := range c.Tasks[taskType] {
		if task.State == TaskRunning && now-task.SentAt > OvertimeLimit {
			task.State = TaskWait
			task.SentAt = -1

			node, ok := c.Nodes[task.NID]
			if !ok {
				log.Println("Node not found", task.NID)
				return
			}
			node.State = NodeFailed
		}
	}
}

func (c *Coordinator) updateIntermediates(files []string) error {
	for _, filename := range files {
		names := strings.Split(filename, "-")

		mapID, err := strconv.Atoi(names[1])
		if err != nil {
			log.Println("Filename format wrong", filename)
			return errors.New("filename format wrong")
		}

		reduceID, err := strconv.Atoi(names[2])
		if err != nil {
			log.Println("Filename format wrong", filename)
			return errors.New("filename format wrong")
		}
		c.Intermediates[reduceID][mapID] = filename
	}
	return nil
}

func (c *Coordinator) fetchTaskFiles(task *Task) []string {
	if task.Type == TaskMapType {
		return []string{c.MapFiles[task.ID]}
	}

	result := []string{}
	for _, filename := range c.Intermediates[task.ID] {
		result = append(result, filename)
	}
	return result
}

func (c *Coordinator) initializeTasks(files []string, nReduce int) {
	c.Tasks[TaskMapType] = make(map[int]*Task)
	c.Tasks[TaskReduceType] = make(map[int]*Task)
	for i, filename := range files {
		c.MapFiles[i] = filename
		c.Tasks[TaskMapType][i] = &Task{
			ID:         i,
			Type:       TaskMapType,
			State:      TaskWait,
			SentAt:     -1,
			FinishedAt: -1,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.Tasks[TaskReduceType][i] = &Task{
			ID:         i,
			Type:       TaskReduceType,
			State:      TaskWait,
			SentAt:     -1,
			FinishedAt: -1,
		}
	}
}

func (c *Coordinator) initializeIntermediates(nReduce int) {
	for i := 0; i < nReduce; i++ {
		c.Intermediates[i] = make(map[int]string)
	}
}
