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

func TestStartSequentialServer(t* testing.T) {
	StartServers(Sequential)

	log.Printf("Waiting for 3 servers to bootup...\n\n")
	time.Sleep(time.Millisecond * 500)
}

func TestSequential1(t *testing.T) {
	// testing with three clients
	var clients [3]*services.Client
	for i := 0; i < 3; i++ {
		clients[i] = &services.Client{ServerIface: Cfg.ClientPorts[i], TrackVersion: false}
	}

	var wg sync.WaitGroup

	// Read from server 2 after 5 milliseconds
	// Meanwhile write request sent to server 1
	// This result should follow linearizable order
	wg.Add(1)
	go func() {
		time.Sleep(time.Millisecond * 2)
		defer wg.Done()
		clients[1].Read("x")
	}()

	// write to server 1
	clients[0].Write("x", "1")

	// Read from server 3
	wg.Add(1)
	go func() {
		defer wg.Done()
		v, _ := clients[2].Read("x")
		if v != "1" {
			log.Fatal("Test falied")
		}
	}()

	wg.Wait()
}

func TestSequentialOrder(t *testing.T) {
	var clients [3]*services.Client
	for i := 0; i < 3; i++ {
		clients[i] = &services.Client{ServerIface: Cfg.ClientPorts[i], TrackVersion: false}
	}

	go func() {
		clients[0].Write("a", "1")
	}()
	
	go func() {
		clients[1].Write("a", "2")
	}()

	time.Sleep(time.Millisecond * 50)

	clients[1].Read("a")
	clients[2].Read("a")
	
}

func TestSequentialPerformance(t *testing.T) {
	var clients [3]*services.Client
	for i := 0; i < 3; i++ {
		clients[i] = &services.Client{ServerIface: Cfg.ClientPorts[i], TrackVersion: false}
	}

	// 1000 successive set and get requests
	// reading my own write
	for i := 0; i < 10; i++ {
		client := clients[i % 3]
		key := "x"
		val := fmt.Sprintf("%d", i)
		client.Write(key, val)
		client = clients[(i + 1) % 3]
		res, _ := client.Read(key)
		if res != val {
			t.Fatalf("Cannot read last write!")
		}
	}
}

func TestSeqParallelRequests(t *testing.T) {
	var clients [3]*services.Client
	for i := 0; i < 3; i++ {
		clients[i] = &services.Client{ServerIface: Cfg.ClientPorts[i], TrackVersion: false}
	}

	// 1000 successive set and get requests
	// reading my own write
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
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