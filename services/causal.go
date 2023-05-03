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
	- Causal order broadcast
*/
func StartCausalServer(clientIface, serverIface, kvStoreIface string) error {
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
			// log.Printf("Received msg %s at %s\n", string(buffer), serverIface)

			go func(conn net.Conn) {
				defer conn.Close()
				buffer := make([]byte, cfg.PayloadSize)
				size, _ := conn.Read(buffer)

				message := map[string]string{}
				json.Unmarshal(buffer[:size], &message)
				message["op"] = strings.ToLower(message["op"])

				// only write messages are broadcasted
				if message["op"] == "set" {
					// check for dependency write!
					msgDependency, ok := message["dependency"]
					if ok && msgDependency != "{}" && msgDependency != "" {
						dependency := make(map[string]string)
						json.Unmarshal([]byte(msgDependency), &dependency)

						// wait for until the dependent write operation is done
						// before appyling the current write operation
						for {
							time.Sleep(time.Millisecond * time.Duration(rand.Intn(20)))
							mu.Lock()
							valStr, err := kvStore.Get(ctx, dependency["key"]).Result()
							mu.Unlock()
							if err != redis.Nil {
								val := make(map[string]string)
								json.Unmarshal([]byte(valStr), &val)
								// dependent write is complete
								// log.Printf("Val: %s, Dependency: %s at %s\n", valStr, msgDependency, serverIface)
								if val["version"] == dependency["version"] {
									// log.Printf("Dependent write %s version %s done\n", dependency["key"], dependency["version"])
									break
								}
							}
						}
					}

					mu.Lock()
					// log.Printf("Writing %v at %s\n", message, serverIface)
					val, err := kvStore.Get(ctx, message["key"]).Result()

					// no key exists
					var newVersion int
					if err == redis.Nil {
						newVersion = 0
					} else {
						result := make(map[string]string)
						json.Unmarshal([]byte(val), &result)
						newVersion, _ = strconv.Atoi(result["version"])
					}
					newVersion++

					rawObj, _ := json.Marshal(map[string]string{
						"value": message["value"],
						"version": strconv.Itoa(newVersion),
					})

					kvStore.Set(ctx, message["key"], string(rawObj), 0)
					mu.Unlock()

				}
				conn.Close()

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
				mu.Lock()
				val, err := kvStore.Get(ctx, message["key"]).Result()
				mu.Unlock()

				if message["op"] == "set" {
					log.Printf("%d Start : Write %s = %s  at server %s\n", 
						timestamp, message["key"], message["value"], clientIface)
					// no key exists
					var newVersion int
					if err == redis.Nil {
						newVersion = 0
					} else {
						result := make(map[string]string)
						json.Unmarshal([]byte(val), &result)
						newVersion, _ = strconv.Atoi(result["version"])
					}
					newVersion++

					rawObj, _ := json.Marshal(map[string]string{
						"value": message["value"],
						"version": strconv.Itoa(newVersion),
					})
	
					mu.Lock()
					kvStore.Set(ctx, message["key"], string(rawObj), 0)
					mu.Unlock()

					jsonMsg, _ := json.Marshal(message)
					// broadcast message and do not include itself!
					BroadcastMsg(jsonMsg, serverIface, false, 8)

					message["version"] = strconv.Itoa(newVersion)
					log.Printf("%d End   : Write %s = %s version %s at server %s\n",
						time.Now().UnixMilli(), message["key"], message["value"], message["version"], clientIface)

				} else if message["op"] == "get" {
					log.Printf("%d Start : Read %s with min version %s at server %s\n", 
						timestamp, message["key"], message["minVersion"], clientIface)
					// minimum version
					result := make(map[string]string)
					currentVersion := -1
					minVersion, _ := strconv.Atoi(message["minVersion"])

					for currentVersion < minVersion {
						time.Sleep(time.Millisecond * 5)

						mu.Lock()
						val, err := kvStore.Get(ctx, message["key"]).Result()
						mu.Unlock()

						if err == redis.Nil {
							message["value"] = "nil"
							currentVersion = 0
							break
						} else {
							json.Unmarshal([]byte(val), &result)
							message["value"] = result["value"]
							currentVersion, _ = strconv.Atoi(result["version"])
						}
					}

					message["version"] = strconv.Itoa(currentVersion)
					log.Printf("%d End   : Read %s = %s version %s at server %s\n",
						time.Now().UnixMilli(), message["key"], message["value"], message["version"], clientIface)
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