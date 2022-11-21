package proc

import "github.com/squareup/pranadb/shakti"

// RemoteForwarder forwards a WriteBatch to another processor to be enqueued, when the WriteBatch has reached the
// other processor, and then be successfully replicated to replicas, and a response received from the other processor
// will the Committed() callback be called
type RemoteForwarder interface {
	EnqueueRemotely(processorID uint64, batch *shakti.WriteBatch) error
}
