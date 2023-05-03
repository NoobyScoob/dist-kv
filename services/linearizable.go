package services

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	u "dist-kv/utils"
)

/*
	- Total order broadcast for both reads and writes
	- Ordering is based on wall clock time -> Linearizability
	- Local machine time is used as global wall clock time
*/
func StartLinearizableServer(clientIface, serverIface, kvStoreIface string) error {
	// config accessor
	cfg := u.Config
	ctx := context.Background()

	kvStore, err := u.StartRedisClient(ctx, kvStoreIface)
	if err != nil {
		log.Fatal(err)
	}

	// fmt.Printf("Server listeneing on ports: %s | %s\n", clientIface, serverIface)
	listener, _ := net.Listen(cfg.NetType, cfg.NetAddr + ":" + clientIface)
	intListener, _ := net.Listen(cfg.NetType, cfg.NetAddr + ":" + serverIface) // internal listener

	var mu sync.Mutex
	// tracks message id and ack count
	acks := map[string]int{}
	connQ := make(chan net.Conn, 1000)
	pq := make(u.PriorityQueue, 0)
	heap.Init(&pq)

	// register connection handler for server-to-server broadcasts 
	go func() {
		for {
			conn, _ := intListener.Accept()
			buffer := make([]byte, cfg.PayloadSize)
			size, _ := conn.Read(buffer)

			message := map[string]string{}
			json.Unmarshal(buffer[:size], &message)
			message["op"] = strings.ToLower(message["op"])

			_, isAck := message["ack"]
			// fmt.Printf("At %s received {%s: %s}\n", serverIface, message["op"], message["totalOrderTimestamp"])
			// message is acknowledgement
			if isAck {
				mu.Lock()
				_, ok := acks[message["id"]]
				if ok {
					acks[message["id"]]++
				} else {
					acks[message["id"]] = 1
				}

				// whenever we got an ack, we check whether the message is deliverable
				for pq.Len() > 0 {
					head := heap.Pop(&pq).(*u.Item)
					// received all the acks for the head
					if acks[head.Message["id"]] == cfg.NumServers {
						// fmt.Printf("Processing top of the heap {%s: %s} at %s\n", head.Message["op"], head.Message["totalOrderTimestamp"], serverIface)
						if head.Message["op"] == "set" {
							// write the message
							kvStore.Set(ctx, head.Message["key"], head.Message["value"], 0)
						} else {
							kvStore.Get(ctx, head.Message["key"]).Result()
						} // read the message

						// assuming we don't get acks after we receive all acks
						acks[head.Message["id"]] = -1
						// fmt.Printf("%v\n", acks)
						// fmt.Printf("PQ len %d\n", pq.Len())
					} else {
						heap.Push(&pq, head)
						break
					}
				}

				mu.Unlock()
				// spawn a go routine if all acks are all received

			} else if message["op"] == "set" || message["op"] == "get" {
				// Generating total order based on process id
				// strips the unix timestamp for testing
				totOrderTimestamp := message["totalOrderTimestamp"]
				timestamp, _ := strconv.ParseFloat(totOrderTimestamp, 64)

				mu.Lock()
				heap.Push(&pq, &u.Item{
					Message: message,
					Priority: timestamp,
				})
				top := heap.Pop(&pq).(*u.Item)
				// fmt.Printf("Top at PQ on %s is {%s: %f}\n", serverIface, top.Message["op"], top.Priority)
				heap.Push(&pq, top)
				mu.Unlock()

				// serialize message
				message["ack"] = "ok"
				jsonMsg, _ := json.Marshal(message)

				// broadcast ack to all the other servers including itself!
				BroadcastMsg(jsonMsg, serverIface, true, 5)
			}
			conn.Close()
		}
	}()

	// connection handler for client-to-server messages
	go func() {
		for conn := range connQ {
			// handle connection here
			buffer := make([]byte, cfg.PayloadSize)
			size, _ := conn.Read(buffer)

			// format {op: 'set', key: key, value: value}
			// format {op: 'get', key: key}
			message := make(map[string]string)
			json.Unmarshal(buffer[:size], &message)

			message["op"] = strings.ToLower(message["op"])
			// add unique message id
			message["id"] = strconv.Itoa(rand.Int())
			// add timestamp to the request
			timestamp := time.Now().UnixMilli()
			message["timestamp"] = strconv.FormatInt(timestamp, 10)
			message["totalOrderTimestamp"] = fmt.Sprintf("%s.%s", message["timestamp"][5:], serverIface)

			// Both read and write are blocking operations
			if message["op"] == "set" || message["op"] == "get" {
				msgBytes, _ := json.Marshal(message)
				BroadcastMsg(msgBytes, serverIface, true, 5)

				if message["op"] == "set" {
					log.Printf("%d Start : Write %s = %s at server %s\n", timestamp, message["key"], message["value"], clientIface)
				} else {
					log.Printf("%d Start : Read %s at server %s\n", timestamp, message["key"], clientIface)
				}

				// commit the message here
				for {
					time.Sleep(time.Millisecond * 10)
					mu.Lock()
					ackCount, ok := acks[message["id"]]
					// fmt.Printf("Ack count for  %d\n", ackCount)
					mu.Unlock()
					// when all acks are received, before updating the last ack
					// separate thread updates the database based on the priority queue
					if ok && (ackCount == -1) { break }
				}

				if message["op"] == "get" {
					mu.Lock()
					val, err := kvStore.Get(ctx, message["key"]).Result()
					if err != nil {
						log.Printf("Redis error: %v\n", err)
					}
					mu.Unlock()
					message["value"] = val
				}

				message["errors"] = ""

				if message["op"] == "set" {
					log.Printf("%d End   : Write %s = %s at server %s\n", time.Now().UnixMilli(), message["key"], message["value"], clientIface)
				} else {
					log.Printf("%d End   : Read %s = %s at server %s\n",time.Now().UnixMilli(), message["key"], message["value"], clientIface)
				}

			} else {
				message["error"] = "Client Error!"
			}

			res, _ := json.Marshal(message)
			conn.Write(res)
			conn.Close()
		}
	}()

	// handle connections
	for {
		conn, _ := listener.Accept()
		connQ <- conn
	}
}

// broadcasts messages to every other node
// if from string is empty self broadcast is avoided!
func BroadcastMsg(message []byte, from string, self bool, delay int) {
	cfg := u.Config
	for i := 0; i < cfg.NumServers; i++ {
		if !self && cfg.ServerPorts[i] == from { continue }
		go func(to string) {
			dist := distance(from, to)
			duration := math.Pow(float64(delay), float64(dist))
			time.Sleep(time.Millisecond * time.Duration(duration))
			// log.Printf("Sending msg %s from  %s to %s\n", string(message), from, to)
			conn, _ := net.Dial(cfg.NetType, cfg.NetAddr + ":" + to)
			defer conn.Close()
			conn.Write(message)
		}(cfg.ServerPorts[i])
	}
}

func KillAll() {
	cfg := u.Config
	for i := 0; i < cfg.NumServers; i++ {
		u.KillRedisClient(cfg.KvStorePorts[i])
	}
}

// Used to generate delays as distance between
// from addresss and to address
func distance(from, to string) int {
	fromInt, _ := strconv.Atoi(from)
	toInt, _ := strconv.Atoi(to)
	dist := fromInt - toInt
	if dist < 0 {
		dist = -1 * dist
	}
	return dist
}