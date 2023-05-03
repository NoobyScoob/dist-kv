BINARY=bin/server

build:
	GOARCH=amd64 GOOS=linux go build -o ${BINARY} server.go 

test-linearizable:
	go test -v kv_linearizable_test.go  server.go

test-sequential:
	go test -v kv_sequential_test.go  server.go

test-eventual:
	go test -v kv_eventual_test.go  server.go

test-causal:
	go test -v kv_causal_test.go  server.go

test: test-linearizable

clean:
	go clean
	rm bin/*