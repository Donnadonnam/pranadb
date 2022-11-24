package mem

import (
	"bytes"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/shakti/arenaskl"
	"github.com/squareup/pranadb/shakti/cmn"
	"github.com/squareup/pranadb/shakti/iteration"
	"sync"
)

type DeleteRange struct {
	StartKey []byte
	EndKey   []byte
}

const batchSizeEntriesEstimate = 256

var initialOverhead, avgOverHeadPerEntry = calcEntryMemOverhead()

func NewBatch() *Batch {
	return &Batch{
		entries: make([]cmn.KV, 0, batchSizeEntriesEstimate),
	}
}

type Batch struct {
	totKVSize    int
	entries      []cmn.KV // This is so we preserve insertion order
	deleteRanges []DeleteRange
}

func (b *Batch) AddEntry(kv cmn.KV) {
	b.entries = append(b.entries, kv)
	b.totKVSize += len(kv.Key) + len(kv.Value)
}

func (b *Batch) Serialize(buff []byte) []byte {
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(b.entries)))
	for _, kv := range b.entries {
		buff = common.AppendUint32ToBufferLE(buff, uint32(len(kv.Key)))
		buff = append(buff, kv.Key...)
		buff = common.AppendUint32ToBufferLE(buff, uint32(len(kv.Value)))
		buff = append(buff, kv.Value...)
	}
	// TODO delete ranges
	return buff
}

func (b *Batch) Size() int {
	return len(b.entries)
}

func (b *Batch) Deserialize(buff []byte, offset int) int {
	l, offset := common.ReadUint32FromBufferLE(buff, offset)
	b.entries = make([]cmn.KV, l)
	for i := 0; i < int(l); i++ {
		var lk uint32
		lk, offset = common.ReadUint32FromBufferLE(buff, offset)
		b.totKVSize += int(lk)
		k := buff[offset : offset+int(lk)]
		offset += int(lk)
		var lv uint32
		lv, offset = common.ReadUint32FromBufferLE(buff, offset)
		var v []byte
		if lv > 0 {
			b.totKVSize += int(lv)
			v = buff[offset : offset+int(lv)]
		}
		offset += int(lv)
		b.entries[i].Key = k
		b.entries[i].Value = v
	}
	// TODO delete ranges
	return offset
}

// Memtable TODO range deletes
type Memtable struct {
	arena              *arenaskl.Arena
	sl                 *arenaskl.Skiplist
	commonPrefix       []byte
	writeIter          arenaskl.Iterator
	first              bool
	committedCallbacks sync.Map
}

func NewMemtable(arena *arenaskl.Arena) *Memtable {
	sl := arenaskl.NewSkiplist(arena)
	mt := &Memtable{
		arena: arena,
		sl:    sl,
		first: true,
	}
	mt.writeIter.Init(sl)
	return mt
}

func (m *Memtable) Write(batch *Batch, committedCallback func(error) error) (bool, error) {

	spaceRemaining := int(m.arena.Cap() - m.arena.Size())
	var estimateMemSize int
	if m.first {
		// The first entry allocates a bunch of stuff in the skiplist so is higher
		estimateMemSize = initialOverhead
	}
	estimateMemSize += len(batch.entries)*avgOverHeadPerEntry + 32 // 32 byte fudge factor to reduce likelihood of arena full
	estimateMemSize += batch.totKVSize
	log.Debugf("estimate size %d remaining %d", estimateMemSize, spaceRemaining)
	if spaceRemaining < estimateMemSize {
		// No room in the memtable
		return false, nil
	}

	for _, kv := range batch.entries {
		if m.commonPrefix == nil {
			m.commonPrefix = kv.Key
		} else {
			commonPrefixLen := findCommonPrefix(kv.Key, m.commonPrefix)
			if len(m.commonPrefix) != commonPrefixLen {
				m.commonPrefix = m.commonPrefix[:commonPrefixLen]
			}
		}
		var err error
		if err = m.writeIter.Add(kv.Key, kv.Value, 0); err != nil {
			if err == arenaskl.ErrRecordExists {
				err = m.writeIter.Set(kv.Value, 0)
			}
		}
		if err != nil {
			return false, err
		}
	}
	m.first = false
	m.committedCallbacks.Store(&committedCallback, struct{}{})
	return true, nil
}

func findCommonPrefix(key1 []byte, key2 []byte) int {
	lk1 := len(key1) //nolint:ifshort
	lk2 := len(key2) //nolint:ifshort
	var l int
	if lk1 < lk2 {
		l = lk1
	} else {
		l = lk2
	}
	var i int
	for i = 0; i < l; i++ {
		if key1[i] != key2[i] {
			break
		}
	}
	return i
}

func (m *Memtable) NewIterator(keyStart []byte, keyEnd []byte) iteration.Iterator {
	var it arenaskl.Iterator
	it.Init(m.sl)
	if keyStart == nil {
		it.SeekToFirst()
	} else {
		it.Seek(keyStart)
	}
	endOfRange := false
	if keyEnd != nil && it.Valid() && bytes.Compare(it.Key(), keyEnd) >= 0 {
		endOfRange = true
	}
	return &MemtableIterator{
		it:          &it,
		keyStart:    keyStart,
		keyEnd:      keyEnd,
		endOfRange:  endOfRange,
		initialSeek: it.Valid(),
	}
}

func (m *Memtable) CommonPrefix() []byte {
	return m.commonPrefix
}

func (m *Memtable) Committed() error {
	// The memtable has been successfully committed to storage we now call all committed callbacks
	var err error
	m.committedCallbacks.Range(func(key, _ interface{}) bool {
		cb := key.(*(func(error) error)) //nolint:forcetypeassert
		f := *cb
		if err = f(nil); err != nil {
			return false
		}
		return true
	})
	return err
}

type MemtableIterator struct {
	it          *arenaskl.Iterator
	prevIt      *arenaskl.Iterator
	keyStart    []byte
	keyEnd      []byte
	endOfRange  bool
	initialSeek bool
}

func (m *MemtableIterator) Current() cmn.KV {
	if !m.it.Valid() {
		panic("not valid")
	}
	k := m.it.Key()
	v := m.it.Value()
	if len(v) == 0 {
		v = nil
	}
	return cmn.KV{
		Key:   k,
		Value: v,
	}
}

func (m *MemtableIterator) Next() error {
	// we make a copy of the iter before advancing in case we advance off the end (invalid) and later
	// more records arrive
	prevCopy := *m.it
	m.it.Next()
	if m.keyEnd != nil && bytes.Compare(m.it.Key(), m.keyEnd) >= 0 {
		// end of range
		m.endOfRange = true
	}
	m.prevIt = &prevCopy
	return nil
}

func (m *MemtableIterator) IsValid() (bool, error) {
	if !m.initialSeek {
		if m.keyStart == nil {
			m.it.SeekToFirst()
		} else {
			m.it.Seek(m.keyStart)
		}
		if m.it.Valid() {
			m.initialSeek = true
		}
	}
	if m.endOfRange {
		return false, nil
	}
	if m.it.Valid() {
		return true, nil
	}
	// We have to cache the previous value of the iterator before we moved to nil node (invalid)
	// that's where new entries will be added
	if m.prevIt != nil {
		cp := *m.prevIt
		m.prevIt.Next()
		if m.prevIt.Valid() {
			// There are new entries - reset the iterator to prev.next
			m.it = m.prevIt
			m.prevIt = nil
			return true, nil
		}
		m.prevIt = &cp
	}
	return false, nil
}

func (m *MemtableIterator) Close() error {
	return nil
}

func calcEntryMemOverhead() (int, int) {
	arena := arenaskl.NewArena(uint32(65536))
	sl := arenaskl.NewSkiplist(arena)
	var iter arenaskl.Iterator
	iter.Init(sl)
	initialOverhead := 0
	totalOtherOverhead := 0
	numIters := 1000
	prevSize := 0
	for i := 0; i < numIters; i++ {
		key := common.AppendUint64ToBufferBE([]byte{0, 0}, uint64(i))
		value := make([]byte, 10)
		err := iter.Add(key, value, 0)
		if err != nil {
			panic(err)
		}
		s := arena.Size()
		diff := s - uint32(prevSize)
		overhead := diff - 20
		if i == 0 {
			// initial overhead is higher
			initialOverhead = int(overhead)
		} else {
			totalOtherOverhead += int(overhead)
		}
		prevSize = int(s)
	}
	avgOverheadPerRow := totalOtherOverhead / (numIters - 1)
	return initialOverhead, avgOverheadPerRow
}
