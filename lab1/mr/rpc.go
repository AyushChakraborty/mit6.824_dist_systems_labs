package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskRequest struct{}

type TaskReply struct {
	TaskType   string //can be "map" or "reduce" or "wait" or "exit"
	TaskNumber int    //which map task or reduce task is being allotted to that worker
	Filename   string
	NReduce    int //needed for the map tasks, total number of reducers
}

type TaskDoneRequest struct {
	TaskNumber int
	TaskType   string
}

type TaskDoneReply struct {}

type WorkerToMasterReq struct {
	ReqType string
}

type MasterToWorkerRes struct {
	Res    string
	FileID int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
