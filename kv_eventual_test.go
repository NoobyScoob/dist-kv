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

func TestStartEventualServer(t* testing.T) {
	StartServers(Eventual)

	log.Printf("Waiting for 3 servers to bootup...\n\n")
	time.Sleep(time.Millisecond * 500)
}

func TestEventual(t *testing.T) {
	// testing with three clients
	var clients [3]*services.Client
	for i := 0; i < 3; i++ {
		clients[i] = &services.Client{ServerIface: Cfg.ClientPorts[i], TrackVersion: false}
	}

	// write to server 1
	clients[0].Write("x", "1")

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		start := time.Now()
		var v1, v2 string
		for {
			time.Sleep(time.Millisecond * 50)
			v1, _ = clients[1].Read("x")
			v2, _ = clients[2].Read("x")
			if (v1 == "1") && (v2 == "1") { break }
		}
		log.Printf("Took %v to read x = %s from %s and %s\n", time.Since(start), v1, clients[1].ServerIface, clients[2].ServerIface)
		wg.Done()
	}()

	wg.Wait()
	
}

func TestEventualParallelPerformance(t *testing.T) {
	var clients [3]*services.Client
	for i := 0; i < 3; i++ {
		clients[i] = &services.Client{ServerIface: Cfg.ClientPorts[i], TrackVersion: false}
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
			client = clients[(i + 1) % 3]
			client.Read(key)
			// if res != val {
			// 	log.Fatalf("Cannot read last write!")
			// }
		} (i)
	}

	wg.Wait()
}