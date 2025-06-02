package master

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Pranay-ai/mapreduce-go/storage"
)

// MapTask represents a task assignment
type MapTask struct {
	TaskID   string
	FilePath string
}

type AssignMapTaskResponse struct {
	TaskID        string
	TaskType      string
	InputFilePath string
}

type MasterNode struct {
	inputFilePath         string
	outputFilePath        string
	numReducers           int
	workers               []Worker
	pluginFilePath        string
	nfsStoragePath        string
	metadataFilePath      string
	mapTasks              map[string]int
	mapTaskChannel        chan MapTask
	intermediateFilePaths map[string]string
	chunkMetadata         map[string]string // NEW: maps taskID to original split input file path
}

func NewMasterNode(inputFilePath, outputFilePath string, numReducers int, pluginFilePath, nfsStoragePath string) *MasterNode {
	return &MasterNode{
		inputFilePath:         inputFilePath,
		outputFilePath:        outputFilePath,
		numReducers:           numReducers,
		workers:               make([]Worker, 0),
		pluginFilePath:        pluginFilePath,
		nfsStoragePath:        nfsStoragePath,
		metadataFilePath:      "",
		mapTasks:              make(map[string]int),
		intermediateFilePaths: make(map[string]string),
		chunkMetadata:         make(map[string]string), // Initialize it here!
	}
}

func (m *MasterNode) AddWorker(worker Worker) {
	m.workers = append(m.workers, worker)
}

func (m *MasterNode) SplitFileIntoChunks(chunkSize int) error {
	fileUtility := storage.FileUtility{
		InputFilePath:  m.inputFilePath,
		OutputFilePath: filepath.Join(m.nfsStoragePath, "splits"),
	}
	err := fileUtility.SplitFileIntoChunks(chunkSize)
	if err != nil {
		return err
	}

	hashedDir := fileUtility.GetHashedDirName()
	metadataFilePath := filepath.Join(fileUtility.OutputFilePath, hashedDir, "metadata.json")

	m.metadataFilePath = metadataFilePath
	return nil
}

func (m *MasterNode) LoadMapTasksFromMetadata() error {
	if m.metadataFilePath == "" {
		return fmt.Errorf("metadata file path not set")
	}

	file, err := os.Open(m.metadataFilePath)
	if err != nil {
		return fmt.Errorf("failed to open metadata file: %w", err)
	}
	defer file.Close()

	var metadata storage.FileMetadata
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&metadata); err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}

	tempChannel := make(chan MapTask, len(metadata.Chunks))

	for _, chunk := range metadata.Chunks {
		m.mapTasks[chunk.ChunkID] = 0                   // Pending
		m.chunkMetadata[chunk.ChunkID] = chunk.FilePath // Populate the chunk metadata map

		tempChannel <- MapTask{
			TaskID:   chunk.ChunkID,
			FilePath: chunk.FilePath,
		}
	}

	m.mapTaskChannel = tempChannel

	return nil
}

func (m *MasterNode) AssignMapTask(workerID string) (*AssignMapTaskResponse, error) {
	select {
	case task := <-m.mapTaskChannel:
		m.mapTasks[task.TaskID] = 1 // In Progress

		for i, worker := range m.workers {
			if worker.workerID == workerID {
				m.workers[i].tasks = append(m.workers[i].tasks, task.TaskID)
				break
			}
		}

		return &AssignMapTaskResponse{
			TaskID:        task.TaskID,
			TaskType:      "map",
			InputFilePath: task.FilePath,
		}, nil
	default:
		return nil, fmt.Errorf("no tasks available for assignment")
	}
}

func (m *MasterNode) SubmitMapTask(workerID, taskID, intermediateFilePath string) error {
	if _, exists := m.mapTasks[taskID]; !exists {
		return fmt.Errorf("task ID %s not found", taskID)
	}

	if m.mapTasks[taskID] != 1 { // Check if task is in progress
		return fmt.Errorf("task ID %s is not in progress", taskID)
	}

	m.mapTasks[taskID] = 2 // Mark as completed
	m.intermediateFilePaths[taskID] = intermediateFilePath

	for i, worker := range m.workers {
		if worker.workerID == workerID {
			m.workers[i].lastPingTime = time.Now()
			break
		}
	}

	return nil
}

func (m *MasterNode) PingFromWorker(workerID string) error {
	for i, worker := range m.workers {
		if worker.workerID == workerID {
			m.workers[i].lastPingTime = time.Now()
			return nil
		}
	}
	return fmt.Errorf("worker with ID %s not found", workerID)
}

func (m *MasterNode) CheckWorkerTimeouts(timeoutDuration time.Duration) {
	for i := len(m.workers) - 1; i >= 0; i-- {
		if time.Since(m.workers[i].lastPingTime) > timeoutDuration {
			m.MarkWorkerAsDead(m.workers[i].workerID)
		}
	}
}

func (m *MasterNode) MarkWorkerAsDead(workerID string) {
	for i, worker := range m.workers {
		if worker.workerID == workerID {
			fmt.Printf("Worker %s marked as dead\n", workerID)

			for _, taskID := range worker.tasks {
				if _, exists := m.mapTasks[taskID]; exists {
					m.mapTasks[taskID] = 0 // Pending
					filePath, ok := m.chunkMetadata[taskID]
					if !ok {
						fmt.Printf("Original file path for task %s not found, skipping reassignment\n", taskID)
						continue
					}
					m.mapTaskChannel <- MapTask{
						TaskID:   taskID,
						FilePath: filePath, // Use split input file path
					}
					fmt.Printf("Task %s reassigned to task channel\n", taskID)
				}
			}

			m.workers = append(m.workers[:i], m.workers[i+1:]...)
			return
		}
	}
	fmt.Printf("Worker %s not found to mark as dead\n", workerID)
}
