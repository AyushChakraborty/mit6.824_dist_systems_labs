package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
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

	for {
		reply := Call()

		if reply.TaskType == "map" {
			data, err := os.ReadFile(reply.Filename)
			if err != nil {
				log.Fatal(err)
			}

			intermediate_pairs := mapf(reply.Filename, string(data))

			// I will save the intermediate files as mr-X-Y where X is the file Idx as defined by the master,
			// Y is the hashed reducer number

			//file per reducer
			reducer_files := make(map[int]*os.File)

			for _, pair := range intermediate_pairs {
				Y := ihash(pair.Key) % reply.NReduce

				// get or create file for this bucket
				if reducer_files[Y] == nil {
					filename := fmt.Sprintf("mr-%d-%d", reply.TaskNumber, Y)
					file, err := os.Create(filename)
					if err != nil {
						log.Fatal(err)
					}
					reducer_files[Y] = file
				}
				// write to the bucket file
				fmt.Fprintf(reducer_files[Y], "%v %v\n", pair.Key, pair.Value)
			}

			for _, file := range reducer_files {
				file.Close()
			}
			TaskDoneCall(reply.TaskNumber, reply.TaskType)

		} else if reply.TaskType == "reduce" {
			reducerNum := reply.TaskNumber

			//find all the intermediate map results alloted to this reducer
			pattern := fmt.Sprintf("mr-*-%d", reducerNum)
			matches, err := filepath.Glob(pattern)
			if err != nil {
				log.Fatal(err)
				continue
			}

			//collecting all intermediate key value pairs from all intermediate files
			intermediate := []KeyValue{}

			for _, filename := range matches {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatal(err)
					continue
				}

				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					line := scanner.Text()
					parts := strings.SplitN(line, " ", 2)
					if len(parts) == 2 {
						intermediate = append(intermediate, KeyValue{
							Key:   parts[0],
							Value: parts[1],
						})
					}
				}
				file.Close()
			}

			if len(intermediate) == 0 {
				TaskDoneCall(reply.TaskNumber, reply.TaskType)
				continue //dont clutter with empty files from unused reducers
			}

			sort.Sort(ByKey(intermediate))

			outputFilename := fmt.Sprintf("mr-out-%d", reducerNum)
			outputFile, err := os.Create(outputFilename)
			if err != nil {
				log.Fatal(err)
				continue
			}

			//call reduce func for each unique key
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}

				//collect all values for this key
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}

				output := reducef(intermediate[i].Key, values)
				fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			outputFile.Close()
			TaskDoneCall(reply.TaskNumber, reply.TaskType)

		} else if reply.TaskType == "wait" {
			continue
		} else if reply.TaskType == "exit" {
			//fmt.Print("Exiting the worker process...\n")
			break //where the loop breaks and the worker shuts down hence
		}
	}
}

func Call() TaskReply {
	args := TaskRequest{}

	reply := TaskReply{}

	call("Master.AssignTask", &args, &reply)

	//fmt.Printf("file to process as sent by master: %s\n", reply.Filename)
	return reply
}

func TaskDoneCall(taskNum int, taskType string) TaskDoneReply {
	args := TaskDoneRequest{TaskNumber: taskNum, TaskType: taskType}

	reply := TaskDoneReply{}

	call("Master.ReceiveStatus", &args, &reply)
	return reply
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

	//fmt.Println(err)
	return false
}
