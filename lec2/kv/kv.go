package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
)

// standard definitions
const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

type PutArgs struct {
	Key string
	Val string
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

// define the KV type and its methods. These methods are the ones exposed via RPC
// shared memory model used
type KV struct {
	mu   sync.Mutex
	data map[string]string
}

// methods have this specific signature following the Go RPC server rules, where signature must be
// func (t *Type) Method(args *ArgsType, reply *ReplyType) error
func (kv *KV) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	kv.data[args.Key] = args.Val
	reply.Err = OK
	kv.mu.Unlock()
	return nil
}

func (kv *KV) Get(arg *GetArgs, reply *GetReply) error {
	kv.mu.Lock()

	val, ok := kv.data[arg.Key]
	if ok {
		reply.Value = val
		reply.Err = OK
	} else {
		reply.Value = ""
		reply.Err = ErrNoKey
	}

	kv.mu.Unlock()
	return nil
}

// server
func server() {
	kv := new(KV)
	kv.data = map[string]string{}

	rpcs := rpc.NewServer()
	rpcs.Register(kv) //registers the type which exposes its methods via RPC

	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error: ", e) //exits the program too
	}
	go func() {
		for { //due to this unbounded for, there is no need to wait for any of the goroutines, can be assured that main wont exit first
			conn, err := l.Accept()
			if err == nil {
				go rpcs.ServeConn(conn)
			} else {
				break //if server not able to accept the conn from client, server just stops
			}
		}
		l.Close()
	}()
}

//client, also has get and put functions, they give a RPC call to Get and Put

func connect() *rpc.Client {
	client, err := rpc.Dial("tcp", ":1234")
	if err != nil {
		log.Fatal("dialing error: ", err)
	}
	return client
}

func get(key string) string {
	client := connect()
	args := GetArgs{key}
	reply := GetReply{}

	err := client.Call("KV.Get", &args, &reply) //after the RPC returns, the client unmarshals
	//the servers response bytes to the client's reply struct
	if err != nil {
		log.Fatal("get error: ", err)
	}
	client.Close()
	return reply.Value
}

func put(key string, val string) {
	client := connect()
	args := PutArgs{key, val}
	reply := PutReply{}

	err := client.Call("KV.Put", &args, &reply)
	if err != nil {
		log.Fatal("put error: ", err)
	}
	client.Close()
}

func main() {
	server() //in a sep go routine, so this main can be used for the client loop

	//client loop
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("type help")
	for {
		fmt.Printf("kv.go> ")

		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)

		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		cmd := parts[0]

		switch cmd {
		case "help":
			fmt.Println("to put key:value pair> put key:value")
			fmt.Println("to get value of key>    get key")

		case "put":
			if len(parts) < 2 {
				fmt.Println("usage: put key:value")
				continue
			}
			sep := strings.Split(parts[1], ":")
			if len(sep) < 2 {
				fmt.Println("usage: put key:value")
				continue
			}
			put(sep[0], sep[1])
			fmt.Println("Done")

		case "get":
			if len(parts) < 2 {
				fmt.Println("usage: get key")
				continue
			}
			val := get(parts[1])
			fmt.Println(val)

		default:
			fmt.Println("unknown command:", cmd)
		}
	}
}
