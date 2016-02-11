package mapreduce

import (
	"fmt"
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

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	
	// available workers to take up tasks
	var available = safeStringSet{m: make(map[string]struct{})}

	// in-progress tasks
	var in_progress = safeMap{m: make(map[int]struct{})}

	// tasks that were assigned but failed
	var failed = safeMap{m: make(map[int]struct{})}

	var next_task = struct{
		mux sync.RWMutex
		v int
	}{v: 0}

	for _, wrk := range mr.workers {
		available.m[wrk] = struct{}{}
	}

	for {
		select {
		// Case 1: Process new worker registrations if they exist
		case wker := <- mr.registerChannel:
			fmt.Printf("[Added] worker %v to list\n", wker)
			mr.workers = append(mr.workers, wker)
			available.mux.Lock()
			available.m[wker] = struct{}{}
			available.mux.Unlock()
		// Otherwise: Appoint leftover tasks to current workers
		default:
			// If all tasks have completed
			in_progress.mux.RLock()
			failed.mux.RLock()
			next_task.mux.RLock()
			num_failed := len(failed.m)
			no_more_tasks := next_task.v == ntasks

			if no_more_tasks && len(in_progress.m) == 0 && num_failed == 0{
				next_task.mux.RUnlock()
				failed.mux.RUnlock()
				in_progress.mux.RUnlock()
				return
			}
			// fmt.Printf("Here 2\n")
			next_task.mux.RUnlock()
			failed.mux.RUnlock()
			in_progress.mux.RUnlock()

			if num_failed > 0 || !no_more_tasks {
				// fmt.Printf("Here 3\n")
				// Pick a random free worker to assign a task to
				available.mux.Lock()
				var worker string
				for key, _ := range available.m {
					worker = key
					delete(available.m, key)
					break
				}
				available.mux.Unlock()

				// fmt.Printf("Here 4\n")
				if worker != "" {
					// Pick the next task
					task := -1;
					if len(failed.m) > 0 {
						failed.mux.Lock()
						for key, _ := range failed.m {
							task = key
							delete(failed.m, key)
							break
						}
						failed.mux.Unlock()
					} else {
						next_task.mux.Lock()
						task = next_task.v
						next_task.v++
						next_task.mux.Unlock()
					}

					if task >= 0 && task < ntasks {
						in_progress.mux.Lock()
						in_progress.m[task] = struct{}{}
						in_progress.mux.Unlock()

						do_task_args := DoTaskArgs{JobName:mr.jobName, File: mr.files[task], Phase:phase, 
												   TaskNumber:task, NumOtherPhase:nios }
						
						go appointTask(worker, &do_task_args, &available, &in_progress, &failed)
					}
				}
			}
			
		}
	}
}

func appointTask(worker string, taskArgs *DoTaskArgs, 
				 available *safeStringSet,
				 in_progress *safeMap, 
				 failed *safeMap) {
	ok := call(worker, "Worker.DoTask", taskArgs, new(struct{})) 

	// Add worker back to available
	available.mux.Lock()
	available.m[worker] = struct{}{}
	available.mux.Unlock()

	// Remove task from in-progress
	in_progress.mux.Lock()
	delete(in_progress.m, taskArgs.TaskNumber)
	in_progress.mux.Unlock()
	
	// Add task to failed
	if ok == false {
		failed.mux.Lock()
		failed.m[taskArgs.TaskNumber] = struct{}{}
		failed.mux.Unlock()
	}
}
