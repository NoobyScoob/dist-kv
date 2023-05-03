package utils

import (
	"context"
	"log"
	"os/exec"

	"github.com/redis/go-redis/v9"
)

func StartRedisClient(ctx context.Context, port string) (*redis.Client, error) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	// start redis database server
	// log.Printf("Starting redis server on: %s\n", port)
	cmd := exec.Command("redis-server", "--port", port, "--daemonize", "yes")
	err := cmd.Run()
	if err != nil {
		log.Printf("Issue with starting redis server on: %s\n", port)
		return nil, err
	}

	// fmt.Printf("Connecting to redis server on: %s\n", port)
	kvStore := redis.NewClient(&redis.Options{
        Addr: "localhost:" + port,
        Password: "",
        DB: 0,
    })

	// drop any old db
	kvStore.FlushAll(ctx)

	return kvStore, nil
}

func KillRedisClient(port string) {
	cmd := exec.Command("redis-cli", "-p", port, "shutdown")
	err := cmd.Run()
	if err != nil {
		log.Printf("Issue with killing redis server on: %s\n", port)
	}
}