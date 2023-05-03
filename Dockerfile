# Use an official Golang runtime as a parent image
FROM golang:1.20

RUN apt-get update && \
    apt-get install -y redis-server

# Set the working directory to /go/src/app
WORKDIR /go/src/app

# Copy the current directory contents into the container at /go/src/app
COPY . /go/src/app

# Download and install any required dependencies
RUN go get -d -v ./...

# Run tests
RUN go test -v kv_linearizable_test.go  server.go

RUN	go test -v kv_sequential_test.go  server.go

RUN	go test -v kv_eventual_test.go  server.go

RUN	go test -v kv_causal_test.go  server.go

# Run the application when the container starts
CMD ["app"]