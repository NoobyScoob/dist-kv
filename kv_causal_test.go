package distkv

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"dist-kv/services"
)

// tests may not execute in the sequential order
// so we may have to check if servers are started in every test

func TestStartCausalServer(t* testing.T) {
	StartServers(Causal)

	log.Printf("Waiting for 3 servers to bootup...\n\n")
	time.Sleep(time.Millisecond * 500)
}

func TestCausality(t *testing.T) {
	// testing with three clients
	var clients [3]*services.Client
	for i := 0; i < 3; i++ {
		clients[i] = &services.Client{}
		clients[i].Init(Cfg.ClientPorts[i], true)
	}

	// broadcast takes around 5ms time for second node
	// 25ms for third node
	clients[0].Write("x", "1")

	// wait 11 seconds to get updated x
	time.Sleep(time.Millisecond * 6)
	clients[1].Read("x")
	clients[1].Write("y", "1")

	// if y is read as 1, x must be 1
	time.Sleep(time.Millisecond * 60)
	clients[2].Read("y")
	clients[2].Read("x")
}

func TestCausalityConcurrency(t *testing.T) {
	// testing with three clients
	var clients [3]*services.Client
	for i := 0; i < 3; i++ {
		clients[i] = &services.Client{}
		clients[i].Init(Cfg.ClientPorts[i], true)
	}

	clients[0].Write("a", "1")
	// multicast takes around 10ms time
	// this should return stale y and x
	clients[1].Write("a", "2")

	clients[2].Read("a")
	time.Sleep(time.Millisecond * 50)
	clients[2].Read("a")
	time.Sleep(time.Millisecond * 50)
	clients[2].Read("a")
	clients[0].Read("a")
}

func TestReadMinVersionSequence(t *testing.T) {
	var clients [3]*services.Client
	for i := 0; i < 3; i++ {
		clients[i] = &services.Client{}
		clients[i].Init(Cfg.ClientPorts[i], true)
	}

	// 1000 successive set and get requests
	// reading my own write
	for i := 0; i < 10; i++ {
		client := clients[i % 3]
		key := "x"
		val := fmt.Sprintf("%d", i)
		client.Write(key, val)
		client = clients[(i + 1) % 3]
		client.Read(key)
	}
}

func TestCausalParallelConnections(t *testing.T) {
	var clients [3]*services.Client
	for i := 0; i < 3; i++ {
		clients[i] = &services.Client{}
		clients[i].Init(Cfg.ClientPorts[i], true)
	}

	// 1000 successive set and get requests
	// reading my own write
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(i int) {
			time.Sleep(time.Millisecond * 50)
			defer wg.Done()
			client := clients[i % 3]
			key := "a"
			val := fmt.Sprintf("%d", i)
			client.Write(key, val)
			// client = clients[(i + 1) % 3]
			// client.Read(key)
			// if res != val {
			// 	log.Fatalf("Cannot read last write!")
			// }
		} (i)
	}

	wg.Wait()
}