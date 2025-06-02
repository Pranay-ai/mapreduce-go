package master

import (
	"fmt"
	"log"

	"google.golang.org/grpc"
)

type Worker struct {
	worker_id   int
	worker_addr string
	worker_port int
	conn        *grpc.ClientConn
}

func NewWorker(worker_id int, worker_addr string, worker_port int) *Worker {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", worker_addr, worker_port), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to worker: %v", err)
	}
	return &Worker{
		worker_id:   worker_id,
		worker_addr: worker_addr,
		worker_port: worker_port,
		conn:        conn,
	}
}
