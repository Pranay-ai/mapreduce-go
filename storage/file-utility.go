package storage

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type FileUtility struct {
	InputFilePath  string
	OutputFilePath string
}

// ChunkMetadata holds metadata for a single chunk
type ChunkMetadata struct {
	ChunkID  string `json:"chunk_id"`
	FilePath string `json:"file_path"`
}

// FileMetadata holds metadata for all chunks of a file
type FileMetadata struct {
	OriginalFileName string          `json:"original_file_name"`
	Chunks           []ChunkMetadata `json:"chunks"`
}

// SplitFileIntoChunks splits the input file into chunks and generates metadata
func (fu *FileUtility) SplitFileIntoChunks(chunkSize int) error {
	// Step 1: Hash the input file name
	hash := sha1.Sum([]byte(fu.InputFilePath))
	dirName := hex.EncodeToString(hash[:])

	// Step 2: Create the output directory
	outputDir := filepath.Join(fu.OutputFilePath, dirName)
	err := os.MkdirAll(outputDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Step 3: Open the input file
	inputFile, err := os.Open(fu.InputFilePath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer inputFile.Close()

	// Step 4: Read and split into chunks
	reader := bufio.NewReader(inputFile)
	buffer := make([]byte, chunkSize)
	chunkNumber := 0

	// Collect metadata
	var metadata FileMetadata
	metadata.OriginalFileName = filepath.Base(fu.InputFilePath)

	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			chunkID := fmt.Sprintf("chunk_%d", chunkNumber)
			chunkFileName := chunkID
			chunkFilePath := filepath.Join(outputDir, chunkFileName)

			chunkFile, err := os.Create(chunkFilePath)
			if err != nil {
				return fmt.Errorf("failed to create chunk file: %w", err)
			}

			_, writeErr := chunkFile.Write(buffer[:n])
			if writeErr != nil {
				chunkFile.Close()
				return fmt.Errorf("failed to write chunk file: %w", writeErr)
			}
			chunkFile.Close()

			// Add to metadata
			metadata.Chunks = append(metadata.Chunks, ChunkMetadata{
				ChunkID:  chunkID,
				FilePath: chunkFilePath,
			})

			chunkNumber++
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			return fmt.Errorf("error reading input file: %w", err)
		}
	}

	// Step 5: Save metadata.json in the output directory
	metadataFilePath := filepath.Join(outputDir, "metadata.json")
	metadataFile, err := os.Create(metadataFilePath)
	if err != nil {
		return fmt.Errorf("failed to create metadata file: %w", err)
	}
	defer metadataFile.Close()

	encoder := json.NewEncoder(metadataFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(metadata); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}
