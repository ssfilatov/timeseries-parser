package storage

import (
	"context"
	"fmt"
	"github.com/ssfilatov/ts/pkg/partition"
	"github.com/ssfilatov/ts/pkg/processor"
	"golang.org/x/sync/errgroup"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

const (
	partitionDir = "partitions"
)

type Storage struct {
	mu sync.Mutex
	partitionsByFile map[string][]partition.Partition
}

func (s *Storage) SetFilePartitions(filename string, partitions []partition.Partition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partitionsByFile[filename] = partitions
}

func (s *Storage) GetPartitionsByFilename(filename string) ([]partition.Partition, bool) {
	partitons, found := s.partitionsByFile[filename]
	return partitons, found
}

func buildPath(dir, filename string) string {
	return fmt.Sprintf("%s/%s", dir, filename)
}

func NewStorage(ctx context.Context, partitionSize int, dir string) (*Storage, error) {
	if _, err := os.Stat(partitionDir); os.IsNotExist(err) {
		if err := os.Mkdir(partitionDir, os.ModePerm); err != nil {
			return nil, fmt.Errorf("error creating dir: %v", err)
		}
	}

	partitionProcessor := processor.NewProcessor(partitionSize, partitionDir)
	storage := &Storage{
		partitionsByFile: map[string][]partition.Partition{},
	}

	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatalf("error reading dir: %v", err)
	}
	errs, _ := errgroup.WithContext(ctx)
	for _, fileInfo := range fileInfos {
		fileInfo := fileInfo
		errs.Go(func() error {
			return storage.processFile(dir, fileInfo.Name(), partitionProcessor)
		})
	}
	if err := errs.Wait(); err != nil {
		return nil, err
	}

	return storage, nil
}

func (s *Storage) processFile(dir, fileName string, partitionProcessor *processor.Processor) error {
	file, err := os.Open(buildPath(dir, fileName))
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()
	partitionList, err := partitionProcessor.ProcessRecords(file, fileName)
	if err != nil {
		log.Fatalf("error processing files %s", err)
	}
	s.SetFilePartitions(fileName, partitionList)
	return nil
}
