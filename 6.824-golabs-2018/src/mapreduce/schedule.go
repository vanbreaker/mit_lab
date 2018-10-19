package mapreduce

import (
	"fmt"
	"sync"
)

type workersManager struct {
	sync.Mutex
	regWorkers  []string
	freeWorkers []string
}

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var workersMgr workersManager
	var wg sync.WaitGroup
	nLeftTasks := ntasks
	freeChan := make(chan string)

	go recvRegWorker(&workersMgr, registerChan, freeChan)

	for nLeftTasks > 0 {
		freeCnt := len(workersMgr.freeWorkers)
		//fmt.Printf("free workers %d : %v\n", freeCnt, workersMgr.freeWorkers)
		if freeCnt > 0 {
			worker := workersMgr.freeWorkers[freeCnt-1]
			workersMgr.freeWorkers = workersMgr.freeWorkers[:freeCnt-1]
			taskArg := DoTaskArgs{
				JobName:       jobName,
				Phase:         phase,
				TaskNumber:    ntasks - nLeftTasks,
				NumOtherPhase: n_other,
			}
			if phase == "mapPhase" {
				taskArg.File = mapFiles[ntasks-nLeftTasks]
			}
			wg.Add(1)
			go submitTask(worker, taskArg, freeChan, &wg)
			nLeftTasks--
		} else { // no free worker, wait for new registered worker or job done worker
			//		fmt.Printf("wait for free worker\n")
			fw := <-freeChan
			//		fmt.Printf("recv free worker:%s\n", fw)
			workersMgr.freeWorkers = append(workersMgr.freeWorkers, fw)
			fmt.Printf("free workers:%v\n", workersMgr.freeWorkers)
		}
	}

	for {
		workersMgr.Lock()
		regCnt := len(workersMgr.regWorkers)
		workersMgr.Unlock()
		freeCnt := len(workersMgr.freeWorkers)
		if freeCnt < regCnt {
			fw := <-freeChan
			workersMgr.freeWorkers = append(workersMgr.freeWorkers, fw)
		} else {
			break
		}
	}

	wg.Wait() // wait for all tasks
	fmt.Printf("Schedule: %v done\n", phase)
}

func recvRegWorker(workersMgr *workersManager, registerChan chan string, freeChan chan string) {
	for {
		worker := <-registerChan
		workersMgr.Lock()
		workersMgr.regWorkers = append(workersMgr.regWorkers, worker)
		workersMgr.Unlock()
		//fmt.Printf("register new worker:%s\n", worker)
		freeChan <- worker
	}
}

func submitTask(worker string, arg DoTaskArgs, freeChan chan string, wg *sync.WaitGroup) {
	//fmt.Printf("dispatch task %d to worker %s\n", arg.TaskNumber, worker)
	defer wg.Done()
	ok := call(worker, "Worker.DoTask", arg, new(struct{}))
	if ok == false {
		fmt.Printf("Master: RPC %s doTask error\n", worker)
	} else {
		//fmt.Printf("free worker:%s\n", worker)
		freeChan <- worker
	}
}
