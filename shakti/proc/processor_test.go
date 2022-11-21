package proc

import (
	"github.com/squareup/pranadb/shakti/mem"
	"testing"
)

func TestProcessor(t *testing.T) {
	leader := NewProcessor(1, batchHandler, replicator, forwarder, shakti)
}

type testBatchHandler struct {
}

func (t testBatchHandler) HandleBatch(batch *mem.Batch) (*mem.Batch, map[uint64]*mem.Batch, error) {
	//TODO implement me
	panic("implement me")
}
