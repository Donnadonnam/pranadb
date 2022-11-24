package proc

import (
	"fmt"
	"github.com/squareup/pranadb/shakti/cloudstore"
	"github.com/squareup/pranadb/shakti/cmn"
	"github.com/squareup/pranadb/shakti/datacontroller"
	"github.com/squareup/pranadb/shakti/mem"
	"github.com/squareup/pranadb/shakti/store"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestReplication(t *testing.T) {

	batchHandler := newBatchHandler(nil, nil)

	follower1 := NewProcessor(1, 1000, batchHandler, nil, nil, nil)
	follower2 := NewProcessor(1, 1000, batchHandler, nil, nil, nil)

	shakti := SetupShakti(t)
	replicator := &SimpleReplicator{replicas: []*Processor{follower1, follower2}}

	leader := NewProcessor(1, 1000, batchHandler, replicator, nil, shakti)

	batch := mem.NewBatch()
	for i := 0; i < 10; i++ {
		batch.AddEntry(cmn.KV{
			Key:   []byte(fmt.Sprintf("prefix/key-%010d", i)),
			Value: []byte(fmt.Sprintf("prefix/value-%010d", i)),
		})
	}

	err := leader.RequestEnqueue(0, batch, func(err error) error {
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)
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

// TODO combine these with the ones in shakti test
func SetupShakti(t *testing.T) *store.Shakti {
	t.Helper()
	conf := cmn.Conf{
		MemtableMaxSizeBytes:          1024 * 1024,
		MemtableFlushQueueMaxSize:     4,
		TableFormat:                   cmn.DataFormatV1,
		MemTableMaxReplaceTime:        30 * time.Second,
		DisableBatchSequenceInsertion: true,
	}
	return SetupShaktiWithConf(t, conf)
}

func SetupShaktiWithConf(t *testing.T, conf cmn.Conf) *store.Shakti {
	t.Helper()
	controllerConf := datacontroller.Conf{
		RegistryFormat:                 cmn.MetadataFormatV1,
		MasterRegistryRecordID:         "test.master",
		MaxRegistrySegmentTableEntries: 100,
		LogFileName:                    "shakti_repl.log",
	}
	cloudStore := cloudstore.NewLocalStore(100 * time.Millisecond)
	replicator := &cmn.NoopReplicator{}
	controller := datacontroller.NewController(controllerConf, cloudStore, replicator)
	err := controller.Start()
	require.NoError(t, err)
	shakti := store.NewShakti(1, cloudStore, controller, conf)
	err = shakti.Start()
	require.NoError(t, err)
	err = controller.SetLeader()
	require.NoError(t, err)
	return shakti
}

func stopShakti(t *testing.T, shakti *store.Shakti) {
	err := shakti.Stop()
	require.NoError(t, err)
}
