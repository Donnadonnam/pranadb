package proc

import "github.com/squareup/pranadb/errors"

type Replicator interface {
	ReplicateMessage(processorID uint64, message *replicateMessage) error
}

// LocalReplicator is used in testing
type LocalReplicator struct {
	processors map[uint64]*Processor
}

func (lr *LocalReplicator) ReplicateMessage(processorID uint64, message *replicateMessage) error {
	processor, ok := lr.processors[processorID]
	if !ok {
		return errors.Errorf("cannot find processor %d", processorID)
	}
	if message.batch != nil {
		// It's a batch to replicate
		bytes := message.batch.Serialize(nil)
		if err := processor.ReceiveReplicatedBatch(&replicatedBatch{
			sequenceNum: message.sequenceNum,
			buff:        bytes,
		}); err != nil {
			return err
		}
	} else {
		// It's a commit to replicate
		if err := processor.ReceiveReplicatedCommit(message.sequenceNum); err != nil {
			return err
		}
	}
	return message.completeCallback()
}
