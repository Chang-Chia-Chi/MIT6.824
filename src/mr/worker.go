package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
	pid := os.Getpid()
	register := regist(pid)
	log.Printf("worker %d regist", pid)

	for {
		next := fetchNextTask(register)
		if next.Type == TaskAllDonType {
			log.Printf("Job done, worker %d bye informed by Coordinator!", register.NID)
			os.Exit(1)
		}
		if next.Type == TaskNAType {
			log.Printf("Worker %d: no available job right now", register.NID)
			time.Sleep(time.Second)
			continue
		}

		var files []string
		var err error
		if next.Type == TaskMapType {
			files, err = runMapTask(mapf, next, register.NReduce)
		} else {
			files, err = runReduceTask(reducef, next)
		}
		if err != nil {
			continue
		}
		completeTask(register.NID, next.TID, next.Type, files)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func runMapTask(mapf func(string, string) []KeyValue, next NextReply, nReduce int) ([]string, error) {
	intermediate := map[int][]KeyValue{}
	for _, filename := range next.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return []string{}, err
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
			return []string{}, err
		}
		file.Close()

		kva := mapf(filename, string(content))
		for _, kv := range kva {
			h := ihash(kv.Key) % nReduce
			intermediate[h] = append(intermediate[h], kv)
		}
	}

	files := []string{}
	for i, kva := range intermediate {
		tmp, err := os.CreateTemp("", "map_temp")
		if err != nil {
			log.Fatal("create temporary file failed")
			return []string{}, err
		}

		enc := json.NewEncoder(tmp)
		for _, kv := range kva {
			if err := enc.Encode(&kv); err != nil {
				log.Fatal("encode failed", err)
				return []string{}, err
			}
		}
		tmp.Close()

		output := fmt.Sprintf("mr-%d-%d", next.TID, i)
		os.Rename(tmp.Name(), output)
		files = append(files, output)
	}
	return files, nil
}

func runReduceTask(reducef func(string, []string) string, next NextReply) ([]string, error) {
	kva := []KeyValue{}
	for _, filename := range next.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return []string{}, err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	tmp, err := os.CreateTemp("", "map_temp")
	if err != nil {
		log.Fatal("create temporary file failed", err)
		return []string{}, err
	}
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[i].Key == kva[j].Key {
			j++
		}

		vals := []string{}
		for k := i; k < j; k++ {
			vals = append(vals, kva[k].Value)
		}
		result := reducef(kva[i].Key, vals)
		fmt.Fprintf(tmp, "%v %v\n", kva[i].Key, result)

		i = j
	}
	tmp.Close()

	output := fmt.Sprintf("mr-out-%d", next.TID)
	os.Rename(tmp.Name(), output)
	return []string{output}, nil
}

func completeTask(nid int, tid int, tType int, files []string) CompleteReply {
	args := CompleteArgs{
		NID:   nid,
		TID:   tid,
		Type:  tType,
		Files: files,
	}
	reply := CompleteReply{}
	ok := call("Coordinator.Complete", &args, &reply)
	if !ok {
		log.Printf("Job done, worker %d bye when completeTask!", nid)
		os.Exit(1)
	}
	return reply
}

func regist(pid int) RegisterReply {
	args := RegisterArgs{
		UID: pid,
	}
	reply := RegisterReply{}
	ok := call("Coordinator.Register", &args, &reply)
	if !ok {
		log.Println("Job done, bye!")
		os.Exit(1)
	}
	return reply
}

func fetchNextTask(register RegisterReply) NextReply {
	args := NextArgs{
		NID: register.NID,
	}
	reply := NextReply{}
	ok := call("Coordinator.Next", &args, &reply)
	if !ok {
		log.Printf("Job done, worker %d bye when fetchNextTask!", register.NID)
		os.Exit(1)
	}
	return reply
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
