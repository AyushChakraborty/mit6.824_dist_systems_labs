package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

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

	reply, fileId := Call()
	data, err := os.ReadFile(reply)
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
	intermediate_pairs := mapf(reply, string(data))
	// fmt.Printf("%v\n", intermediate_pairs)
	fmt.Printf("%v\n", reply)
	sort.Sort(ByKey(intermediate_pairs))

	//fmt.Printf("%v\n", intermediate_pairs)
	// I will save the intermediate files as mr-X-Y where X is the file Idx as defined by the master,
	// Y is the hashed reducer number

	currName := ""
	var buffer strings.Builder

	for _, pair := range intermediate_pairs {

		Y := ihash(pair.Key)
		name := "mr-" + strconv.Itoa(fileId) + "-" + strconv.Itoa(Y)

		if currName == "" {
			fmt.Fprintf(&buffer, "%v %v\n", pair.Key, pair.Value)
			currName = name
		} else if name != currName {
			interFileName, _ := os.Create(name)
			fmt.Fprint(interFileName, buffer.String())
			interFileName.Close()
			currName = name
			buffer.Reset() //clear the buffer for the new k-v pairs with the new key
			fmt.Fprintf(&buffer, "%v %v\n", pair.Key, pair.Value)
		} else if name == currName {
			fmt.Fprintf(&buffer, "%v %v\n", pair.Key, pair.Value)
		}
	}

}

func Call() (string, int) {
	args := WorkerToMasterReq{ReqType: "task_need"}

	reply := MasterToWorkerRes{}

	call("Master.SendFilename", &args, &reply)

	fmt.Printf("file to process as sent by master: %s\n", reply.Res)
	return reply.Res, reply.FileID
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
