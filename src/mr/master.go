package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const TempDir = "tmp"
const TaskTimeout = 10

type TaskStatus int
type TaskType int
type JobStage int

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
	ExitTask
)

const (
	NotStarted TaskStatus = iota
	Executing
	Finished
)

type Task struct {
	Type     TaskType
	Status   TaskStatus
	Index    int
	File     string
	WorkerId int
}

type Master struct {
	// Your definitions here.
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nMap        int
	nReduce     int
}

// Your code here -- RPC handlers for the worker to call.

//
// GetReduceCount RPC handler.
//
func (m *Master) GetReduceCount(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	reply.ReduceCount = len(m.reduceTasks)

	return nil
}

//
// RequestTask RPC handler.
//
func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	m.mu.Lock()

	var task *Task
	if m.nMap > 0 {
		task = m.selectTask(m.mapTasks, args.WorkerId)
	} else if m.nReduce > 0 {
		task = m.selectTask(m.reduceTasks, args.WorkerId)
	} else {
		task = &Task{ExitTask, Finished, -1, "", -1}
	}

	reply.TaskType = task.Type
	reply.TaskId = task.Index
	reply.TaskFile = task.File

	// fmt.Println("RequestTask: selected task: ", *task)
	m.mu.Unlock()
	go m.waitForTask(task)

	return nil
}

//
// RequestTask RPC handler.
//
func (m *Master) ReportTaskDone(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var task *Task
	if args.TaskType == MapTask {
		task = &m.mapTasks[args.TaskId]
	} else if args.TaskType == ReduceTask {
		task = &m.reduceTasks[args.TaskId]
	} else {
		fmt.Printf("Incorrect task type to report: %v\n", args.TaskType)
		return nil
	}

	// workers can only report task done if the task was not re-assigned due to timeout
	if args.WorkerId == task.WorkerId && task.Status == Executing {
		// fmt.Printf("Task %v reports done.\n", *task)
		task.Status = Finished
		if args.TaskType == MapTask && m.nMap > 0 {
			m.nMap--
		} else if args.TaskType == ReduceTask && m.nReduce > 0 {
			m.nReduce--
		}
	}

	reply.CanExit = m.nMap == 0 && m.nReduce == 0

	return nil
}

func (m *Master) selectTask(taskList []Task, workerId int) *Task {
	var task *Task

	for i := 0; i < len(taskList); i++ {
		if taskList[i].Status == NotStarted {
			task = &taskList[i]
			task.Status = Executing
			task.WorkerId = workerId
			return task
		}
	}

	return &Task{NoTask, Finished, -1, "", -1}
}

func (m *Master) waitForTask(task *Task) {
	if task.Type != MapTask && task.Type != ReduceTask {
		return
	}

	<-time.After(time.Second * TaskTimeout)

	m.mu.Lock()
	defer m.mu.Unlock()

	if task.Status == Executing {
		task.Status = NotStarted
		task.WorkerId = -1
		// fmt.Println("Task timeout, reset task status: ", *task)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.nMap == 0 && m.nReduce == 0
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	nMap := len(files)
	m.nMap = nMap
	m.nReduce = nReduce
	m.mapTasks = make([]Task, 0, nMap)
	m.reduceTasks = make([]Task, 0, nReduce)

	for i := 0; i < nMap; i++ {
		mTask := Task{MapTask, NotStarted, i, files[i], -1}
		m.mapTasks = append(m.mapTasks, mTask)
	}
	for i := 0; i < nReduce; i++ {
		rTask := Task{ReduceTask, NotStarted, i, "", -1}
		m.reduceTasks = append(m.reduceTasks, rTask)
	}

	m.server()

	// clean up and create temp directory
	outFiles, _ := filepath.Glob("mr-out*")
	for _, f := range outFiles {
		if err := os.Remove(f); err != nil {
			log.Fatalf("Cannot remove file %v\n", f)
		}
	}
	err := os.RemoveAll(TempDir)
	if err != nil {
		log.Fatalf("Cannot remove temp directory %v\n", TempDir)
	}
	err = os.Mkdir(TempDir, 0755)
	if err != nil {
		log.Fatalf("Cannot create temp directory %v\n", TempDir)
	}

	return &m
}
