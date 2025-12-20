package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// an array of bool, where each index represents the status of a file corresponding to that index
type status struct {
	processing []bool
	processed  []bool
}

type Master struct {
	IdxToFile  map[int]string
	TotalFiles int
	StatusLock sync.Mutex
	Status     status
}

func (m *Master) SendFilename(args *WorkerToMasterReq, reply *MasterToWorkerRes) error {

	m.StatusLock.Lock()

	for idx := range m.TotalFiles {
		fmt.Printf("files seen in the loop: %v, %v, %v, %v\n", idx, m.IdxToFile[idx], m.Status.processed[idx], m.Status.processing[idx])
		if m.Status.processed[idx] == false && m.Status.processing[idx] == false {
			m.Status.processing[idx] = true
			reply.Res = m.IdxToFile[idx]
			reply.FileID = idx
			break
		}
	}

	m.StatusLock.Unlock()

	//have to handle the case where the task is processed
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.TotalFiles = 0

	m.IdxToFile = make(map[int]string)
	for idx, file := range files {
		m.IdxToFile[idx] = file
		m.TotalFiles += 1
	}

	m.Status.processed = make([]bool, m.TotalFiles)
	m.Status.processing = make([]bool, m.TotalFiles)

	fmt.Printf("%v\n", m.IdxToFile)

	m.server()
	return &m
}
