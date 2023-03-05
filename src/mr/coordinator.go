package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

const(
	idle int =0
	in_progress int=1
	completed int=2
)
type Coordinator struct {
	// Your definitions here.
	m int //M map tasks
	r int //R is specified by the user,equaling to nReduce
	mapnumber int
	reducenumber int
	splitname []string
	isMapAssigned []bool
	isReduceAssigned []bool
	state int
	location []int
	size []int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskRequest(args TaskArgs,reply *TaskReply) error {
	if c.mapnumber==0 && c.reducenumber!=0 {//reduce task
		reply.task="reduce"
//		reply.filename=''''
		reply.tasknumber=c.reducenumber
		reply.m=c.m
		reply.r=c.r
		c.reducenumber-=1
		return nil
	}
	if c.mapnumber !=0 {
	//	c.m-=1
		reply.task="map"
		reply.filename=c.splitname[c.mapnumber]
		reply.tasknumber=c.mapnumber
		reply.m=c.m
		reply.r=c.r
		c.mapnumber-=1
		return nil
		
	}
	return 2
}

//func (c *Coordinator)
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
//	ret := true
	// Your code here.
	if c.reducenumber==0 {
		ret=true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.m=len(files)
	c.mapnumber=c.m
	for k,filename = range files {
		c.splitname[k]=filename
		c.isMapAssigned=append(isMapAssigned,false)
		c.isReduceAssigned=append(isReduceAssigned,false)
	}
	c.r=nReduce
	c.reducenumber=c.r
	c.server()
	return &c
}
