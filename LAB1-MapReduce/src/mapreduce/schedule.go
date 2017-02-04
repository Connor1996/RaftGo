package mapreduce

import "fmt"

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
	signals := make(chan bool, ntasks)
	for i := 0; i < ntasks; i++ {
		// must save i for closure to capture
		taskId := i
		go func() {
			ok := false
			for !ok {
				// get a free worker
				worker := <- mr.registerChannel
				// rpc call to finish job
				args := DoTaskArgs{mr.jobName, mr.files[taskId], phase, taskId, nios}
				ok = call(worker, "Worker.DoTask", &args, new(struct{}))
				if !ok {
					fmt.Printf("fail: %v %v tasks (%d I/Os)\n", taskId, phase, nios)
				} else {
					signals <- true
					// return the free worker
					mr.registerChannel <- worker
				}
			}
		}()
	}

	// wait for workers to finish
	for i := 0; i < ntasks; i++ {
		<- signals
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
