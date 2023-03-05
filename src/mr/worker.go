package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "strconv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args:=TaskArgs{}
	reply:=TaskReply{}
	err := call("Coordinator.TaskRequest",&Args,&Reply)
	if err!=nil {
		fmt.Printf("task request failed!\n")
	}

	if reply.task =="map" {
		file,err:=os.open(reply.filename)
		if err!=nil {
			log.Fatalf("can not open %v!\n",reply.filename)
		}
		content, err := ioutil.ReadAll(reply.filename)
		if err!=nil {
			log.Fatalf("can not read %v!\n",reply.filename)
		}
		file.Close()
		kva:=mapf(reply.filename,string(content))

		for i:=1;i<=reply.r;i++ {
			oname:="mr-"
			oname+=strconv.Itoa(reply.m)
			oname+="-"
			oname+=strconv.Itoa(i)
			ofile,err:=os.OpenFile(oname,O_CREATE|O_WRONLY,ModeAppend)
			if err !=nil {
				log.Fatalf("cannot open %v", ofile)
			}
		}
		
		for _,kv = range kva {
			outnumber:=ihash(kv.Key)%reply.r
			ofile:="mr-"
			ofile+=strconv.Itoa(reply.tasknumber)
			ofile+="-"
			ofile+=strconv.Itoa(outnumber)
			fmt.Printf(ofile, "%v %v\n", kv.Key,kv.Value)
		}
		for i:=1;i<reply.r;i++ {
			oname:="mr-"
			oname=oname+strconv.Itoa(reply.m)+strconv.Itoa(i)
			oname.Close()
		}
	}

	if reply.task =="reduce" {
		intermediate:=[]mr.KeyValue{}
		for i:=1;i<=reply.m;i++ {
			oname:="mr-"+strconv.Itoa(i)+strconv.Itoa(reply.tasknumber)
			ofile,err:=os.Open(oname)
			content,err:=ioutil.ReadAll(ofile)
			intermediate=append(intermediate,content...)
			ofile.Close()
		}
		sort.Sort(ByKey(intermediate))

		oname:="mr-out-"+strconv.Itoa(reply.tasknumber)
		ofile,_err:=os.Create(oname)

		i:=0
		for i<len(intermediate) {
			j:=i+1
			for j<len(intermediate)&&intermediate[i].Key==intermediate[j].Key {
				j++
			}
			values:=[]string
			for k:=i;k<j;k++ {
				values = append(values, intermediate[k].Value)
			}
			output:=reducef(intermediate[i].Key,Values)
			fmt.Printf(ofile, "%v %v\n", intermediate[i].Key, output)
			i=j
		}
		ofile.Close()

	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
