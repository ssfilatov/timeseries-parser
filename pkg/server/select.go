package server

import (
	"encoding/json"
	"fmt"
	"github.com/mailru/easyjson"
	"github.com/ssfilatov/ts/pkg/errorx"
	"github.com/ssfilatov/ts/pkg/partition"
	"github.com/ssfilatov/ts/pkg/record"
	"github.com/ssfilatov/ts/pkg/storage"
	"io"
	"log"
	"net/http"
	"sort"
	"time"
)

type SelectRequest struct {
	Filename string
	From string
	To string
}

type handler struct {
	storage *storage.Storage
}

func newHandler(storage *storage.Storage) *handler {
	return &handler{
		storage: storage,
	}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != "/" {
		http.NotFound(w, req)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := writeToken(w, "["); err != nil {
		return
	}
	defer func() {
		_ = writeToken(w, "]")
	}()
	err := h.HandleSelect(w, req)
	if err != nil {
		log.Printf(err.Error())
	}
}

func (h *handler) HandleSelect(w io.Writer, req *http.Request) error {
	var selectReq SelectRequest
	if err := json.NewDecoder(req.Body).Decode(&selectReq); err != nil {
		return errorx.BadRequest(err)
	}

	start, err := time.Parse(time.RFC3339, selectReq.From)
	if err != nil {
		return errorx.BadRequest(err)
	}
	end, err := time.Parse(time.RFC3339, selectReq.To)
	if err != nil {
		return errorx.BadRequest(err)
	}

	partitionsByFilename, found := h.storage.GetPartitionsByFilename(selectReq.Filename)
	if !found {
		return errorx.New(fmt.Sprintf("file %s is not found", selectReq.Filename))
	}

	return h.Select(w, partitionsByFilename, start, end)
}

func writeToken(w io.Writer, s string) error {
	if _, err := w.Write([]byte(s)); err != nil {
		log.Printf(err.Error())
		return err
	}
	return nil
}

// Select uses binary search to look for partitions and returns sorted record slice
func (h *handler) Select(w io.Writer, partitions []partition.Partition,
	start, end time.Time) error {

	startIdx := sort.Search(len(partitions), func(i int) bool {
		return partitions[i].MaxTimestamp() >= start.Unix()
	})
	endIdx := sort.Search(len(partitions), func(i int) bool {
		return partitions[i].MinTimestamp() > end.Unix()
	})
	for i := startIdx; i < endIdx; i++ {
		partitionRecords, err := partitions[i].SelectRecords(start.Unix(), end.Unix())
		if err != nil {
			return errorx.WrapWithMessage(err, "error selecting records")
		}
		if i != startIdx {
			if err := writeToken(w, ","); err != nil {
				return err
			}
		}
		for i, r := range partitionRecords {
			if i != 0 {
				if err := writeToken(w, ","); err != nil {
					return err
				}
			}
			if _, err := easyjson.MarshalToWriter(record.ConvertInternalRecordToAPI(r), w); err != nil {
				return err
			}
		}
	}
	return nil
}
