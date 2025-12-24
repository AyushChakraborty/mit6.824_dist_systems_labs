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

	for {
		reply := Call()

		if reply.TaskType == "map" {
			data, err := os.ReadFile(reply.Filename)
			if err != nil {
				//fmt.Println("Error: ", err)
				os.Exit(1)
			}

			intermediate_pairs := mapf(reply.Filename, string(data))
			// fmt.Printf("%v\n", intermediate_pairs)
			//fmt.Printf("%v\n", reply)
			sort.Sort(ByKey(intermediate_pairs))

			//fmt.Printf("%v\n", intermediate_pairs)
			// I will save the intermediate files as mr-X-Y where X is the file Idx as defined by the master,
			// Y is the hashed reducer number

			currName := ""
			var buffer strings.Builder

			for _, pair := range intermediate_pairs {

				Y := ihash(pair.Key) % reply.NReduce

				name := "mr-" + strconv.Itoa(reply.TaskNumber) + "-" + strconv.Itoa(Y)

				if currName == "" {
					fmt.Fprintf(&buffer, "%v %v\n", pair.Key, pair.Value)
					currName = name
				} else if name != currName {
					interFileName, err := os.Create(name)
					for err != nil {
						log.Fatal(err)
						interFileName, err = os.Create(name) //repeatedly tries to create
						//the intermediate file
					}
					defer interFileName.Close()

					fmt.Fprint(interFileName, buffer.String())
					currName = name
					buffer.Reset() //clear the buffer for the new k-v pairs with the new key
					fmt.Fprintf(&buffer, "%v %v\n", pair.Key, pair.Value)
				} else if name == currName {
					fmt.Fprintf(&buffer, "%v %v\n", pair.Key, pair.Value)
				}
			}
			TaskDoneCall(reply.TaskNumber, reply.TaskType)

		} else if reply.TaskType == "reduce" {
			reducerNum := reply.TaskNumber //the reducer number allotted to this worker

			//search for the intermediate files in the same dir of /lab1 which end with reducerNum.
			// In an actual distributed case, with a central file system, like HDFS or GFS, the files need to
			// be accessed from it, but for this lab, since all the workers and the master run as separate processes
			// in the same system, this suffices

			//collect all such files

			pattern := "mr-*" + strconv.Itoa(reducerNum)
			matches, err := filepath.Glob(pattern)
			if err != nil {
				log.Fatal(err)
				continue //in the case where the reducer does not find any intermediate file
				//associated with it, in which case it coninues and does so in a loop, until the master
				//recognises it and reassigns the task to the same worker again, hopefully this time
				//with the intermediate files actually available
			}

			outputFilename := "mr-out-" + strconv.Itoa(reducerNum)
			outputFile, err := os.Create(outputFilename)
			if err != nil {
				log.Fatal(err)
				continue //again following the same philosophy as mentioned in the comment above
			}
			defer outputFile.Close()

			for _, fileName := range matches {
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatal(err)
					continue
				}
				defer file.Close()

				var values []string
				key := ""
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					line := strings.TrimRight(scanner.Text(), "\n")
					split := strings.Split(line, " ")
					if key == "" {
						key = split[0]
					}
					values = append(values, split[1])
				}

				reducedVal := reducef(key, values)
				fmt.Fprintf(outputFile, "%v %v\n", key, reducedVal)
			}
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
