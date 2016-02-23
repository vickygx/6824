package mapreduce

import (
	"fmt"
	// "math"
	"sync"
)

type safeStringSet struct {
	mux sync.RWMutex
	m map[string]struct{}
}

type safeMap struct {
	mux sync.RWMutex
	m map[int]struct{}
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
	
	// Variables
	completed_tasks := make(map[int]struct{})
	
	// Channels
	tasks := make(chan int, ntasks)
	completed := make(chan int)
	
	// Putting all tasks that need to be completed onto tasks channel
	for i:= 0; i < ntasks; i++ {
		tasks <- i
	}	

	// Put current free workers onto the queue
	for _,worker := range mr.workers {
		select {
		case next_task := <- tasks:
			do_task_args := DoTaskArgs{JobName:mr.jobName, File: mr.files[next_task], Phase:phase, 
									   TaskNumber:next_task, NumOtherPhase:nios }
			go appointTask(worker, &do_task_args, completed, mr.registerChannel, tasks)
		default:
			go registerWorker(worker, mr.registerChannel)
		}
		
	}

	next_worker := ""
	for {
		// Counting completed tasks
		select {
		case completed_task := <- completed:
			completed_tasks[completed_task] = struct{}{}
		default:
		}

		// Setting up next worker
		if next_worker == "" {
			select {
			case next_worker = <-mr.registerChannel:
				mr.workers = append(mr.workers, next_worker)
			default:
				if len(completed_tasks) == ntasks {
					return
				}
			}
		}

		// If there is a next available worker and task, assign
		if next_worker != "" {
			select {
			case next_task := <- tasks:
				do_task_args := DoTaskArgs{JobName:mr.jobName, File: mr.files[next_task], Phase:phase, 
									   TaskNumber:next_task, NumOtherPhase:nios }
				go appointTask(next_worker, &do_task_args, completed, mr.registerChannel, tasks)
				next_worker = ""
			default:
				if len(completed_tasks) == ntasks {
					return
				}
			}	
		}
	}

}

func registerWorker(worker string, register chan string) {
	register <- worker
}

func appointTask(worker string, taskArgs *DoTaskArgs, 
				 completed chan int,  register chan string, tasks chan int) {
	ok := call(worker, "Worker.DoTask", taskArgs, new(struct{})) 
	if ok {
		completed <- taskArgs.TaskNumber
	} else {
		tasks <- taskArgs.TaskNumber
	}
	register <- worker
}
