# MapReduce Implementation - MIT 6.824 Lab 1

## Acknowledgment

This lab is based on MIT 6.824 Spring 2020 Lab 1: MapReduce.
- Course: http://nil.csail.mit.edu/6.824/2020/
- Lab Instructions: http://nil.csail.mit.edu/6.824/2020/labs/lab-mr.html

**Starter code and test framework provided by MIT 6.824 course staff.**
Implementation of the MapReduce coordinator and worker logic in `mr/master.go`, `mr/worker.go`, `mr/worker.go` is my own work.


## Running the Lab

Build the wordcount application first
```
go build -buildmode=plugin mrapps/wc.go
```

Start the master process
```
go run mrmaster.go pg-*.txt
```

In another window, start the worker
```
go run mrworker.go wc.so
```
