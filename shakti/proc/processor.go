package proc

import (
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/shakti"
	"github.com/squareup/pranadb/shakti/mem"
	"sync"
	"sync/atomic"
)

type Processor struct {
	started            bool
	lock               sync.Mutex
	id                 uint64
	queue              []*mem.Batch
	replicatedBatches  []replicatedBatch
	headPos            int
	queueChan          chan struct{}
	maxQueueSize       int
	batchHander        BatchHandler
	replicator         Replicator
	remoteForwarder    RemoteForwarder
	stopWg             sync.WaitGroup
	batchSequence      int64
	shakti             *shakti.Shakti
	queueLock          common.SpinLock
	lastCommittedBatch int64
}

func NewProcessor(id uint64, maxQueueSize int, batchHandler BatchHandler, replicator Replicator,
	remoteForwarder RemoteForwarder, shakti *shakti.Shakti) *Processor {
	return &Processor{
		id:                 id,
		maxQueueSize:       maxQueueSize,
		batchHander:        batchHandler,
		replicator:         replicator,
		remoteForwarder:    remoteForwarder,
		shakti:             shakti,
		lastCommittedBatch: -1,
	}
}

// Start starts the processor processing batches from its queue
// It's called when a processor becomes leader. Non started processors are passive replicas, and they can receive
// batches to queue and receive messages to dequeue, but they do not process messages until they become leader
func (p *Processor) Start() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.started {
		return nil
	}
	p.queueChan = make(chan struct{}, p.maxQueueSize)
	if err := p.maybeRecover(); err != nil {
		return err
	}
	p.stopWg = sync.WaitGroup{}
	p.stopWg.Add(1)
	go p.runLoop()
	return nil
}

func (p *Processor) maybeRecover() error {
	if err := p.shakti.LoadLastBatchSequence(p.id); err != nil {
		return err
	}
	p.queueLock.Lock()
	defer p.queueLock.Unlock()
	// Process any replicated batches that weren't fully committed, however they could already have been stored or
	// replicated, in this case they will be rejected by duplicate detection in storage and when forwarding to other
	// processors again
	if len(p.replicatedBatches) > 0 {
		wg := sync.WaitGroup{}
		wg.Add(len(p.replicatedBatches))
		for _, batch := range p.replicatedBatches {
			memBatch := mem.NewBatch()
			memBatch.Deserialize(batch.buff, 0)
			if err := p.processBatch(batch.sequenceNum, memBatch, func() error {
				wg.Done()
				return nil
			}); err != nil {
				return err
			}
		}
		wg.Wait()
	}
	return nil
}

// Stop stops the processor processing batches from its queue. It's called when a processor is closed or when it
// transitions from leader to non leader
func (p *Processor) Stop() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if !p.started {
		return nil
	}
	close(p.queueChan)
	p.stopWg.Wait()
	return nil
}

func (p *Processor) RequestEnqueue(sequenceNum int64, batch *mem.Batch) error {
	// First we must replicate the batch, when that is complete we will actually queue the batch
	return p.replicator.ReplicateMessage(p.id, newReplicateMessage(sequenceNum, batch, func() error {
		p.enqueue(batch)
		return nil
	}))
}

// Do the actual enqueue - this is called when the batch has been successfully replicated
func (p *Processor) enqueue(batch *mem.Batch) {
	p.queue = append(p.queue, batch)
	p.queueChan <- struct{}{}
}

func (p *Processor) runLoop() {
	if err := p.loop(); err != nil {
		log.Errorf("failure in processing %+v", err)
	}
	p.stopWg.Done()
}

func (p *Processor) loop() error {
	for {
		_, ok := <-p.queueChan
		if !ok {
			return nil
		}
		p.queueLock.Lock()
		batch := p.queue[p.headPos]
		p.queueLock.Unlock()
		p.headPos++
		sequenceNumber := p.batchSequence
		if err := p.processBatch(p.batchSequence, batch, func() error {
			return p.batchCommitted(sequenceNumber)
		}); err != nil {
			return err
		}
		p.batchSequence++
	}
}

func (p *Processor) processBatch(sequenceNumber int64, batch *mem.Batch, completionFunc func() error) error {
	// Send the bath to be processed by the DAG(s)
	localBatch, remoteBatches, err := p.batchHander.HandleBatch(batch)
	if err != nil {
		return err
	}
	fut := newCountDownFuture(sequenceNumber, completionFunc)
	fut.count = int32(len(remoteBatches))
	if localBatch != nil {
		fut.count++
	}
	// We send the remote batches to be enqueued on other processors and the local write batch to
	// be committed on this node. All this is done asynchronously
	// When all of that is done the completionFunc will be called
	for processorID, remoteBatch := range remoteBatches {
		writeBatch := shakti.NewWriteBatch(processorID, fut.sequenceNum, remoteBatch, fut.countDown)
		if err := p.remoteForwarder.EnqueueRemotely(processorID, writeBatch); err != nil {
			return err
		}
	}
	if localBatch != nil {
		writeBatch := shakti.NewWriteBatch(p.id, sequenceNumber, localBatch, fut.countDown)
		if err := p.shakti.Write(writeBatch); err != nil {
			return err
		}
	}
	return nil
}

func newCountDownFuture(sequenceNum int64, completionFunc func() error) *countDownFuture {
	return &countDownFuture{
		sequenceNum:    sequenceNum,
		completionFunc: completionFunc,
	}
}

// countDownFuture calls the completion func when it's count reaches zero
type countDownFuture struct {
	sequenceNum    int64
	count          int32
	completionFunc func() error
}

func (pf *countDownFuture) countDown() error {
	if atomic.AddInt32(&pf.count, -1) == 0 {
		if err := pf.completionFunc(); err != nil {
			return err
		}
	}
	return nil
}

func (p *Processor) batchCommitted(sequenceNum int64) error {
	// A batch has been processed and committed on this node and also enqueued with any remote processors that the
	// DAG(s) decided to forward batches to
	// So now we must replicate the remove from the queue, and then we can remove the entry from the local queue.
	message := newReplicateMessage(sequenceNum, nil, func() error {
		return p.dequeue(sequenceNum)
	})
	return p.replicator.ReplicateMessage(p.id, message)
}

func (p *Processor) dequeue(sequenceNum int64) error {
	// Note that completions must always come back in order - we will sanity check this!
	p.queueLock.Lock()
	defer p.queueLock.Unlock()
	if sequenceNum != p.lastCommittedBatch+1 {
		panic("batch committed out of sequence")
	}
	p.queue = p.queue[1:] // TODO memleak? investigate
	p.headPos--
	p.lastCommittedBatch = sequenceNum
	return nil
}

func (p *Processor) ReceiveReplicatedCommit(sequenceNum int64) error {
	p.queueLock.Lock()
	defer p.queueLock.Unlock()
	// TODO We need to sanity check here that we're removing the correct entry
	p.queue = p.queue[1:]
	return nil
}

func (p *Processor) ReceiveReplicatedBatch(replicatedBatch *replicatedBatch) error {
	p.queueLock.Lock()
	defer p.queueLock.Unlock()
	lrb := len(p.replicatedBatches)
	if lrb > 0 {
		lastSequence := p.replicatedBatches[lrb-1].sequenceNum
		if replicatedBatch.sequenceNum <= lastSequence {
			// We have seen this batch sequence before - this can happen when reprocessing batches on recovery after
			// failure
			return nil
		}
	}
	p.replicatedBatches = append(p.replicatedBatches, *replicatedBatch)
	return nil
}

type BatchHandler interface {
	// HandleBatch processes a batch through DAG(s) and return batch to write in this processor, and map of batches to forward
	// for queueing on other processors
	HandleBatch(batch *mem.Batch) (*mem.Batch, map[uint64]*mem.Batch, error)
}

func newReplicateMessage(sequenceNum int64, batch *mem.Batch, completeCallback func() error) *replicateMessage {
	return &replicateMessage{
		sequenceNum:      sequenceNum,
		batch:            batch,
		completeCallback: completeCallback,
	}
}

type replicatedBatch struct {
	sequenceNum int64
	buff        []byte
}

type replicateMessage struct {
	sequenceNum      int64
	batch            *mem.Batch
	completeCallback func() error
}

func (r *replicateMessage) complete(err error) {
	if err != nil {
		log.Errorf("failed to replicate batch %+v", err)
		return
	}
	if err := r.completeCallback(); err != nil {
		log.Errorf("failed to replicate message %+v", err)
	}
}
