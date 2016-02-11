package mapreduce

import (
	"fmt"
	"math"
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
	
	completed_tasks := make(map[int]struct{})
	task_to_worker := make(map[int]string)
	
	// Putting all tasks that need to be completed onto tasks channel
	tasks := make(chan int, ntasks)
	for i:= 0; i < ntasks; i++ {
		tasks <- i
	}

	// Channel that receives completed tasks
	completed := make(chan int)
	free := make(chan string, int(math.Max(float64(len(mr.workers)), 2)))

	// Put current free workers onto the queue
	for _,worker := range mr.workers {
		free <- worker
	}

	for {
		next_worker := ""
		select {
		case next_worker = <-mr.registerChannel:
			mr.workers = append(mr.workers, next_worker)
			
		case completed_task := <-completed:
			completed_tasks[completed_task] = struct{}{}
			next_worker = task_to_worker[completed_task]
			delete(task_to_worker, completed_task)

		case next_worker = <-free:
		default:
			if len(completed_tasks) == ntasks {
				return
			}
		}

		// If there is a next available worker and task, assign
		if next_worker != "" {
			select {
			case next_task := <- tasks:
				task_to_worker[next_task] = next_worker
				do_task_args := DoTaskArgs{JobName:mr.jobName, File: mr.files[next_task], Phase:phase, 
									   TaskNumber:next_task, NumOtherPhase:nios }
				go appointTask(next_worker, &do_task_args, completed, free, tasks)
			default:
				if len(completed_tasks) == ntasks {
					return
				}
			}	
		}
	}

}

func appointTask(worker string, taskArgs *DoTaskArgs, 
				 completed chan int,  free chan string, tasks chan int) {
	ok := call(worker, "Worker.DoTask", taskArgs, new(struct{})) 
	
	if ok {
		completed <- taskArgs.TaskNumber
	} else {
		tasks <- taskArgs.TaskNumber
		free <- worker
	}
}
