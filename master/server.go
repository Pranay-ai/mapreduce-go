package master

import (
	"context"

	"github.com/Pranay-ai/mapreduce-go/masterapi"
)

type MasterServer struct {
	masterapi.UnimplementedMasterApiServer
	masterNode *MasterNode
}

func NewMasterServer(node *MasterNode) *MasterServer {
	return &MasterServer{
		masterNode: node,
	}
}

func (s *MasterServer) RegisterWorker(ctx context.Context, req *masterapi.RegisterWorkerRequest) (*masterapi.RegisterWorkerResponse, error) {
	worker := Worker{
		workerID:   req.GetWorkerId(),
		workerAddr: req.GetWorkerAddress(),
		workerPort: req.GetWorkerPort(),
	}

	s.masterNode.AddWorker(worker)

	return &masterapi.RegisterWorkerResponse{
		Success: true,
		Message: "Worker registered successfully",
	}, nil
}

func (s *MasterServer) GetMapTask(ctx context.Context, req *masterapi.GetMapTaskRequest) (*masterapi.GetMapTaskResponse, error) {
	task, err := s.masterNode.AssignMapTask(req.GetWorkerId())
	if err != nil {
		return &masterapi.GetMapTaskResponse{}, nil // Empty response when no tasks
	}

	return &masterapi.GetMapTaskResponse{
		TaskId:       task.TaskID,
		TaskType:     task.TaskType,
		TaskDataPath: task.InputFilePath,
	}, nil
}
