package proc

import (
	"github.com/squareup/pranadb/shakti/mem"
	"testing"
)

func TestProcessor(t *testing.T) {

	batchHandler := newBatchHandler(nil, nil)
	replicator := &LocalReplicator{}

	leader := NewProcessor(1, 1000, batchHandler, replicator, forwarder, shakti)
}

func newBatchHandler(localBatch *mem.Batch, remoteBatches map[uint64]*mem.Batch) BatchHandler {
	return &testBatchHandler{localBatch: localBatch, remoteBatches: remoteBatches}
}

func newFailingBatchHandler(err error) BatchHandler {
	return &testBatchHandler{err: err}
}

type testBatchHandler struct {
	localBatch    *mem.Batch
	remoteBatches map[uint64]*mem.Batch
	err           error
}

func (t *testBatchHandler) HandleBatch(batch *mem.Batch) (*mem.Batch, map[uint64]*mem.Batch, error) {
	if t.err != nil {
		return nil, nil, t.err
	}
	return t.localBatch, t.remoteBatches, nil
}
