package storage

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type FileUtility struct {
	InputFilePath  string
	OutputFilePath string
}

// SplitFileIntoChunks splits the input file into chunks of specified size (bytes)
// and stores them in a subdirectory under OutputFilePath with a unique hashed name.
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

	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			chunkFileName := fmt.Sprintf("chunk_%d", chunkNumber)
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
			chunkNumber++
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			return fmt.Errorf("error reading input file: %w", err)
		}
	}

	return nil
}
