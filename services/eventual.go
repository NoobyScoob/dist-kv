package services

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	u "dist-kv/utils"

	"github.com/redis/go-redis/v9"
)

/*
	- Eventually consistent
	- Most practical and loose consistency gaurantees!
*/
func StartEventualServer(clientIface, serverIface, kvStoreIface string) error {
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
	connQ := make(chan net.Conn, 1000)

	// register connection handler for server-to-server broadcasts 
	go func() {
		for {
			conn, _ := intListener.Accept()

			go func(conn net.Conn) {
				defer conn.Close()
				buffer := make([]byte, cfg.PayloadSize)
				size, _ := conn.Read(buffer)

				message := map[string]string{}
				json.Unmarshal(buffer[:size], &message)
				message["op"] = strings.ToLower(message["op"])

				// only write messages are broadcasted
				if message["op"] == "set" {
					mu.Lock()
					kvStore.Set(ctx, message["key"], message["value"], 0)
					mu.Unlock()
				}
			}(conn)
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

			if message["op"] != "set" && message["op"] != "get" {
				message["error"] = "Client Error!"
			} else {
				// Local Write!
				if message["op"] == "set" {
					log.Printf("%d Start : Write %s = %s at server %s\n", 
						timestamp, message["key"], message["value"], clientIface)

					mu.Lock()
					kvStore.Set(ctx, message["key"], message["value"], 0)
					mu.Unlock()

					jsonMsg, _ := json.Marshal(message)

					BroadcastMsg(jsonMsg, serverIface, false, 5)

					log.Printf("%d End   : Write %s = %s at server %s\n",
						time.Now().UnixMilli(), message["key"], message["value"], clientIface)

				} else if message["op"] == "get" {
					// Local Read
					log.Printf("%d Start : Read %s at server %s\n", 
						timestamp, message["key"], clientIface)

					mu.Lock()
					val, err := kvStore.Get(ctx, message["key"]).Result()
					mu.Unlock()
					if err == redis.Nil {
						message["value"] = "nil"
					} else {
						message["value"] = val
					}

					log.Printf("%d End   : Read %s = %s at server %s\n",
						time.Now().UnixMilli(), message["key"], message["value"], clientIface)
				}
	
				message["errors"] = ""
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