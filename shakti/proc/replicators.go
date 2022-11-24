package proc

type Replicator interface {
	ReplicateMessage(processorID uint64, message *replicateMessage) error
}

// SimpleReplicator is used in testing
type SimpleReplicator struct {
	replicas []*Processor
}

func (sr *SimpleReplicator) ReplicateMessage(processorID uint64, message *replicateMessage) error {
	for _, processor := range sr.replicas {
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
	}
	return message.completionFunc(nil)
}
