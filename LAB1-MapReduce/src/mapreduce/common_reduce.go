package mapreduce

import (
	"os"
	"encoding/json"
	"io"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	// prepare input file
	reduceBuffer := make(map[string][]string)
	var kv KeyValue

	// iterate all intermediate files
	for i := 0; i < nMap; i++ {
		inputFileName := reduceName(jobName, i, reduceTaskNumber)
		inFile, err := os.Open(inputFileName)
		defer inFile.Close()
		checkErr(err)
		decoder := json.NewDecoder(inFile)
		for {
			err := decoder.Decode(&kv)
			if err == io.EOF {
				break
			}
			checkErr(err)

			_, ok := reduceBuffer[kv.Key]
			if !ok {
				reduceBuffer[kv.Key] = make([]string, 0)
			}
			reduceBuffer[kv.Key] = append(reduceBuffer[kv.Key], kv.Value)
		}
	}

	// prepare output file
	outputFileName := mergeName(jobName, reduceTaskNumber)
	outFile, err := os.Create(outputFileName)
	defer outFile.Close()
	checkErr(err)
	encoder := json.NewEncoder(outFile)

	// execute user-defined reduce function
	for key, value := range reduceBuffer {
		encoder.Encode(KeyValue{key, reduceF(key, value)})
	}
}
