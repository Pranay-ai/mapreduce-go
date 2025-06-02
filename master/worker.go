package master

import (
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Worker struct {
	workerID     string
	workerAddr   string
	workerPort   string
	conn         *grpc.ClientConn
	tasks        []string  // List of tasks assigned to the worker
	lastPingTime time.Time // Last time the worker pinged the master
}

func NewWorker(workerID, workerAddr, workerPort string) *Worker {
	address := fmt.Sprintf("%s:%s", workerAddr, workerPort)
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to worker %s at %s: %v", workerID, address, err)
	}
	return &Worker{
		workerID:     workerID,
		workerAddr:   workerAddr,
		workerPort:   workerPort,
		conn:         conn,
		tasks:        make([]string, 0),
		lastPingTime: time.Now(),
	}
}
