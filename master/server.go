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
		TaskDataPath: task.InputFilePath,
	}, nil
}
func (s *MasterServer) SubmitMapTask(ctx context.Context, req *masterapi.SubmitMapTaskRequest) (*masterapi.SubmitMapTaskResponse, error) {

	workerId := req.GetWorkerId()
	taskId := req.GetTaskId()
	intermediateFilePath := req.GetResultDataPath()

	err := s.masterNode.SubmitMapTask(workerId, taskId, intermediateFilePath)
	if err != nil {
		return nil, err
	}
	return &masterapi.SubmitMapTaskResponse{
		Success: true,
		Message: "Map task submitted successfully",
	}, nil

}

func (s *MasterServer) Heartbeat(ctx context.Context, req *masterapi.HeartbeatRequest) (*masterapi.HeartbeatResponse, error) {
	workerID := req.GetWorkerId()
	s.masterNode.PingFromWorker(workerID)

	return &masterapi.HeartbeatResponse{
		Success: true,
		Message: "Heartbeat received",
	}, nil
}
