package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	fmt.Printf("doReduce..reduceTask:%d, nMap:%d, output file:%s\n", reduceTask, nMap, outFile)
	var kvs []KeyValue
	for i := 0; i < nMap; i++ {
		filename := reduceName(jobName, i, reduceTask)
		f, err := os.OpenFile(filename, os.O_RDONLY, 0766)
		if err != nil {
			fmt.Printf("open %s err:%v\n", filename, err)
			return
		}

		jsonDec := json.NewDecoder(f)
		for {
			var kv KeyValue
			err := jsonDec.Decode(&kv)
			if err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		f.Close()
	}

	sort.SliceStable(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })

	outf, err := os.OpenFile(outFile, os.O_RDWR|os.O_CREATE, 0766)
	if err != nil {
		fmt.Printf("open outFile error:%v", err)
		return
	}
	defer outf.Close()
	jsonEnc := json.NewEncoder(outf)

	var key string
	var values []string
	for _, kv := range kvs {
		if kv.Key != key { //hit a new key
			if len(values) > 0 { //encode reduce output into output file
				if err := jsonEnc.Encode(KeyValue{key, reduceF(key, values)}); err != nil {
					fmt.Printf("json Encode into file err:%v\n", err)
					return
				}
				values = nil
			}
			key = kv.Key
		}
		values = append(values, kv.Value)
	}
	if len(values) > 0 { //encode reduce output into output file
		if err := jsonEnc.Encode(KeyValue{key, reduceF(key, values)}); err != nil {
			fmt.Printf("json Encode into file err:%v\n", err)
			return
		}
		values = nil
	}

}
