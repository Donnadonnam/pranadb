package proc

import (
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/shakti"
	"sync"
)

// RemoteForwarder forwards a WriteBatch to another processor to be enqueued, when the WriteBatch has reached the
// other processor, and then be successfully replicated to replicas, and a response received from the other processor
// will the Committed() callback be called
type RemoteForwarder interface {
	EnqueueRemotely(processorID uint64, batch *shakti.WriteBatch) error
}

// LocalForwarder is used in testing
type LocalForwarder struct {
	processors sync.Map
}

func (lf *LocalForwarder) EnqueueRemotely(processorID uint64, batch *shakti.WriteBatch) error {
	p, ok := lf.processors.Load(processorID)
	if !ok {
		return errors.Errorf("cannot find processor %d", processorID)
	}
	processor := p.(*Processor) //nolint:forcetypeassert
	return processor.RequestEnqueue(batch.SequenceNum, batch.Batch, batch.CompletionFunc)
}
