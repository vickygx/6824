package mapreduce


import (
	"hash/fnv"
	"io/ioutil"
	"encoding/json"
	"os"
	// "fmt"
)

func check_err(e error) {
    if e != nil {
        panic(e)
    }
}

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	var contentsByte, err = ioutil.ReadFile(inFile)
	check_err(err)
	kvpairs := mapF(inFile, string(contentsByte))

	// Mapping filename to list of kv pairs
	filename_to_kv := make(map[string][]KeyValue)

	// Loop through each kv pair
	for _, kv := range kvpairs {
		// Figure out where the kv pair belongs to
		r := int(ihash(kv.Key)) % nReduce
		filename := reduceName(jobName, mapTaskNumber, int(r))

		// Add it to the map
		filename_to_kv[filename] = append(filename_to_kv[filename], kv)
	}

	// Loop through filenames and create
    for filename := range filename_to_kv {
    	f, err := os.Create(filename)
		defer f.Close()
		check_err(err)

		encoder := json.NewEncoder(f)

		for _, kv := range filename_to_kv[filename] {
			err := encoder.Encode(&kv)
			check_err(err) // TODO: maybe change
		}
    }
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
