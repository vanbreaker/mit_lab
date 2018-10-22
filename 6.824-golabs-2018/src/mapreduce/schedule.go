package mapreduce

import (
	"fmt"
	"sync"
)

type workersManager struct {
	sync.Mutex
	regWorkers    []string
	freeWorkers   []string
	failedWorkers []string
}

type workerNotify struct {
	workerName     string
	isNewRegWorker bool
	taskResult     bool
	taskArg        DoTaskArgs
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
	freeChan := make(chan workerNotify)

	var unsubmitTaskList []DoTaskArgs              //tasks wait to submit
	handlingTaskMap := make(map[string]DoTaskArgs) //tasks handling by workers

	//initialize unsubmit task list
	for i := ntasks; i > 0; i-- {
		taskArg := DoTaskArgs{
			JobName:       jobName,
			Phase:         phase,
			TaskNumber:    i - 1,
			NumOtherPhase: n_other,
		}
		if phase == "mapPhase" {
			taskArg.File = mapFiles[i-1]
		}
		unsubmitTaskList = append(unsubmitTaskList, taskArg)
	}

	go recvRegWorker(&workersMgr, registerChan, freeChan)

	for len(unsubmitTaskList) > 0 || len(handlingTaskMap) > 0 {
		freeCnt := len(workersMgr.freeWorkers)
		//fmt.Printf("free workers %d : %v\n", freeCnt, workersMgr.freeWorkers)
		if freeCnt > 0 {
			//fetch a worker from free workers
			worker := workersMgr.freeWorkers[freeCnt-1]
			workersMgr.freeWorkers = workersMgr.freeWorkers[:freeCnt-1]

			//move an unsubmit task to handling task collection
			taskArg := unsubmitTaskList[len(unsubmitTaskList)-1]
			unsubmitTaskList = unsubmitTaskList[:len(unsubmitTaskList)-1]
			handlingTaskMap[taskArg.JobName] = taskArg
			wg.Add(1)
			go submitTask(worker, taskArg, freeChan, &wg)
		} else { // no free worker, wait for new registered worker or job done worker
			notify := <-freeChan

			//recv new registered worker
			if notify.isNewRegWorker {
				workersMgr.freeWorkers = append(workersMgr.freeWorkers, notify.workerName)
				continue
			}

			if notify.taskResult == true { //worker successfully done the job
				workersMgr.freeWorkers = append(workersMgr.freeWorkers, notify.workerName)
			} else { //worker failed, put the task back to unsubmit task list and drop the worker
				fmt.Printf("worker %s FAILED when doing task %s\n", notify.workerName, notify.taskArg.JobName)
				workersMgr.failedWorkers = append(workersMgr.failedWorkers, notify.workerName)
				unsubmitTaskList = append(unsubmitTaskList, notify.taskArg)
			}

			delete(handlingTaskMap, notify.taskArg.JobName)
		}
	}

	for {
		workersMgr.Lock()
		regCnt := len(workersMgr.regWorkers)
		workersMgr.Unlock()
		freeCnt := len(workersMgr.freeWorkers)
		failedCnt := len(workersMgr.failedWorkers)
		if freeCnt+failedCnt < regCnt {
			fw := <-freeChan
			workersMgr.freeWorkers = append(workersMgr.freeWorkers, fw.workerName)
		} else {
			break
		}
	}

	wg.Wait() // wait for all tasks
	fmt.Printf("Schedule: %v done\n", phase)
}

func recvRegWorker(workersMgr *workersManager, registerChan chan string, freeChan chan workerNotify) {
	for {
		worker := <-registerChan
		workersMgr.Lock()
		workersMgr.regWorkers = append(workersMgr.regWorkers, worker)
		workersMgr.Unlock()
		//fmt.Printf("register new worker:%s\n", worker)
		notify := workerNotify{
			workerName:     worker,
			isNewRegWorker: true,
		}
		freeChan <- notify
	}
}

func submitTask(worker string, arg DoTaskArgs, freeChan chan workerNotify, wg *sync.WaitGroup) {
	fmt.Printf("dispatch task %d to worker %s\n", arg.TaskNumber, worker)
	defer wg.Done()
	ok := call(worker, "Worker.DoTask", arg, new(struct{}))
	if ok == false {
		fmt.Printf("Master: RPC %s doTask error\n", worker)
	}

	notify := workerNotify{
		workerName:     worker,
		isNewRegWorker: false,
		taskResult:     ok,
		taskArg:        arg,
	}

	freeChan <- notify
}
