package mr

import (
	//"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type statusValues int

const (
	Idle statusValues = iota
	InProgress
	Done
)

type TaskStatus struct {
	Status    statusValues
	StartTime time.Time
}

type Master struct {
	mu          sync.Mutex
	phase       string //"map", "reduce", "done"
	nReduce     int
	mapTasks    []TaskStatus
	files       []string //number of files are equal to the number of map tasks in this lab
	reduceTasks []TaskStatus
}

func (m *Master) allMapTasksDone() bool {
	for _, task := range m.mapTasks {
		if task.Status != Done {
			return false
		}
	}
	return true
}

func (m *Master) allReduceTasksDone() bool {
	for _, task := range m.reduceTasks {
		if task.Status != Done {
			return false
		}
	}
	return true
}

func (m *Master) AssignTask(args *TaskRequest, reply *TaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.phase == "map" {
		for i, task := range m.mapTasks {
			//check if the task at hand is idle OR task is in progress and it is the same way for more than
			// 10s, then assign this task to the new worker
			if task.Status == Idle || (task.Status == InProgress && time.Since(task.StartTime) > 10*time.Second) {
				reply.TaskType = "map"
				reply.TaskNumber = i
				reply.Filename = m.files[i]
				reply.NReduce = m.nReduce

				m.mapTasks[i].Status = InProgress
				m.mapTasks[i].StartTime = time.Now()
				return nil
			}
		}
		if m.allMapTasksDone() {
			//fmt.Print("\nall map tasks done\n")
			m.phase = "reduce"
			//fmt.Printf("%v\n", m.phase)
		}
		reply.TaskType = "wait"

	} else if m.phase == "reduce" {
		for i, task := range m.reduceTasks {
			if task.Status == Idle || (task.Status == InProgress && time.Since(task.StartTime) > 10*time.Second) {
				reply.TaskType = "reduce"
				reply.TaskNumber = i

				m.reduceTasks[i].Status = InProgress
				m.reduceTasks[i].StartTime = time.Now()
				return nil
			}
		}
		if m.allReduceTasksDone() {
			m.phase = "done"
		}
		reply.TaskType = "wait"

	} else {
		reply.TaskType = "exit"
	}

	return nil
}

func (m *Master) ReceiveStatus(args *TaskDoneRequest, reply *TaskDoneReply) error {
	if args.TaskType == "map" {
		m.mapTasks[args.TaskNumber].Status = Done
		if m.allMapTasksDone() {
			m.phase = "reduce"
		}
	} else if args.TaskType == "reduce" {
		m.reduceTasks[args.TaskNumber].Status = Done
		if m.allReduceTasksDone() {
			m.phase = "done"
		}
	}
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
	return m.phase == "done"
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.files = files
	numOfFiles := len(files)
	m.mapTasks = make([]TaskStatus, numOfFiles)
	for i := range m.mapTasks {
		m.mapTasks[i].Status = Idle
		m.mapTasks[i].StartTime = time.Now()
	}
	m.nReduce = nReduce
	m.phase = "map"
	m.reduceTasks = make([]TaskStatus, nReduce)
	for i := range m.reduceTasks {
		m.reduceTasks[i].Status = Idle
		m.reduceTasks[i].StartTime = time.Now()
	}

	//fmt.Printf("%v\n", m.files)
	//fmt.Printf("%v\n", m.mapTasks)
	//fmt.Printf("%v\n", m.reduceTasks)

	m.server()
	return &m
}
