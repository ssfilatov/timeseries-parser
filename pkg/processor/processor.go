package processor

import (
	"bufio"
	"fmt"
	"github.com/ssfilatov/ts/pkg/partition"
	"github.com/ssfilatov/ts/pkg/record"
	"github.com/ugorji/go/codec"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	msgpackHandler codec.MsgpackHandle
)

type Processor struct {
	partitionSize int
	partitionDirPath string

	count int
}

func NewProcessor(partitionSize int, partitionDirPath string) *Processor {
	return &Processor{
		partitionSize: partitionSize,
		partitionDirPath: partitionDirPath,
	}
}

func recordFromString(s string) (*record.InternalRecord, error) {
	tokens := strings.Split(s, " ")
	if len(tokens) != 3 {
		return nil, fmt.Errorf("error parsing record %s", s)
	}
	ts, err := time.Parse(time.RFC3339, tokens[0])
	if err != nil {
		return nil, fmt.Errorf("error parsing record %s", err)
	}
	return &record.InternalRecord{
		Timestamp: ts.Unix(),
		Email: tokens[1],
		SessionID: tokens[2],
	}, nil
}

func (p *Processor) encodeToFile(path string, v interface{}) error {
	metaFile, err := os.Create(path)
	defer metaFile.Close()
	if err != nil {
		return err
	}
	metaEncoder := codec.NewEncoder(metaFile, &msgpackHandler)
	return metaEncoder.Encode(v)
}

func buildDataFilePath(origFilename, dir string, idx int) string {
	return fmt.Sprintf("%s/%s-%s-%s", dir, origFilename, partition.DataFileName, strconv.Itoa(idx))
}

func buildMetaFilePath(origFilename, dir string, idx int) string {
	return fmt.Sprintf("%s/%s-%s-%s", dir, origFilename, partition.MetaFileName, strconv.Itoa(idx))
}

func (p *Processor) scanChunk(scanner *bufio.Scanner) ([]*record.InternalRecord, *partition.Meta, error) {
	records := make([]*record.InternalRecord, 0, p.partitionSize)
	for i := 0; i < p.partitionSize; i++ {
		if ok := scanner.Scan(); !ok {
			break
		}
		r, err := recordFromString(scanner.Text())
		if err != nil {
			log.Print(err.Error())
			continue
		}
		records = append(records, r)
	}
	if scanner.Err() != nil {
		return nil, nil, scanner.Err()
	}
	if len(records) == 0 {
		return nil, nil, nil
	}
	return records, &partition.Meta{
		MinTimestamp: records[0].Timestamp,
		MaxTimestamp: records[len(records) - 1].Timestamp,
		Size: len(records),
	}, nil
}

func (p *Processor) processPartition(dir, origFilename string, partitionIndex int,
	scanner *bufio.Scanner) (partition.Partition, error) {

	records, meta, err := p.scanChunk(scanner)
	if err != nil {
		return nil, err
	}
	if scanner.Err() != nil {
		return nil, scanner.Err()
	}
	if len(records) == 0 {
		return nil, nil
	}

	dataPath := buildDataFilePath(origFilename, dir, partitionIndex)
	if err := p.encodeToFile(dataPath, records); err != nil {
		return nil, err
	}

	metaPath := buildMetaFilePath(origFilename, dir, partitionIndex)
	if err := p.encodeToFile(metaPath, meta); err != nil {
		return nil, err
	}

	return partition.NewPartition(dataPath, metaPath), nil
}

// ProcessRecords splits incoming lines from reader into multiple partition records and writes them to disk
//
// Partition records are encoded to json and written to disk. This method returns slice of partition objects which
// are mmaped to data files on disk.
// prefix arg will be used as a partition file prefix
func (p *Processor) ProcessRecords(r io.Reader, prefix string) ([]partition.Partition, error) {
	scanner := bufio.NewScanner(r)
	var partitionIndex int
	partitionList := make([]partition.Partition, 0)
	for {
		part, err := p.processPartition(p.partitionDirPath, prefix, partitionIndex, scanner)
		if err != nil {
			return nil, err
		}
		if part == nil {
			break
		}
		if err := part.Setup(); err != nil {
			return nil, err
		}
		partitionList = append(partitionList, part)
		partitionIndex++
	}
	return partitionList, nil
}
