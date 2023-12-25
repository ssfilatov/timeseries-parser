package partition

import (
	"bytes"
	"fmt"
	"github.com/ssfilatov/ts/pkg/record"
	"github.com/ugorji/go/codec"
	"os"
	"sort"
	"syscall"
)

const (
	DataFileName = "data"
	MetaFileName = "meta"
)

var (
	msgpackHandler codec.MsgpackHandle
)

type Partition interface {
	MinTimestamp() int64
	MaxTimestamp() int64
	SelectRecords(start, end int64) ([]*record.InternalRecord, error)
	Setup() error
}

type partition struct {
	meta Meta
	mappedFile []byte

	metaPath string
	dataPath string
}

func mmap(fd, length int) ([]byte, error) {
	return syscall.Mmap(
		fd,
		0,
		length,
		syscall.PROT_READ,
		syscall.MAP_SHARED,
	)
}

type Meta struct {
	MinTimestamp  int64
	MaxTimestamp  int64
	Size int
}

func (p *partition) MinTimestamp() int64 {
	return p.meta.MinTimestamp
}

func (p *partition) MaxTimestamp() int64 {
	return p.meta.MaxTimestamp
}

func selectBinary(start, end int64, records []*record.InternalRecord) []*record.InternalRecord {
	startIdx := sort.Search(len(records), func(i int) bool {
		return records[i].Timestamp >= start
	})
	endIdx := sort.Search(len(records), func(i int) bool {
		return records[i].Timestamp > end
	})
	return records[startIdx:endIdx]
}

// SelectRecords returns slice of records that are >= start and <= end sorted by timestamp
func (p *partition) SelectRecords(start, end int64) ([]*record.InternalRecord, error) {
	if end < p.meta.MinTimestamp || start > p.meta.MaxTimestamp {
		return []*record.InternalRecord{}, nil
	}

	partitionRecords := make([]*record.InternalRecord, 0)
	decoder := codec.NewDecoder(bytes.NewReader(p.mappedFile), &msgpackHandler)
	if err := decoder.Decode(&partitionRecords); err != nil {
		return nil, fmt.Errorf("failed to decode data: %w", err)
	}

	return selectBinary(start, end, partitionRecords), nil
}

func NewPartition(dataPath, metaPath string) *partition {
	return &partition{
		metaPath: metaPath,
		dataPath: dataPath,
		meta: Meta{},
	}
}

// Setup sets up meta object and mmaps the data file
func (p *partition) Setup() error {
	f, err := os.Open(p.dataPath)
	if err != nil {
		return fmt.Errorf("failed to read data file: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to fetch file info: %w", err)
	}
	if info.Size() == 0 {
		return fmt.Errorf("empty partition file")
	}
	mapped, err := mmap(int(f.Fd()), int(info.Size()))
	if err != nil {
		return fmt.Errorf("failed to perform mmap: %w", err)
	}

	m := Meta{}
	mf, err := os.Open(p.metaPath)
	if err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}
	defer mf.Close()
	decoder := codec.NewDecoder(mf, &msgpackHandler)
	if err := decoder.Decode(&m); err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}
	p.mappedFile = mapped
	p.meta = m
	return nil
}
