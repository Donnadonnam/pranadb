package mem

import (
	"fmt"
	"github.com/andy-kimball/arenaskl"
	"github.com/squareup/pranadb/shakti/cmn"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func BenchmarkMemTableWrites(b *testing.B) {
	numEntries := 1000
	batch := NewBatch()
	for i := 0; i < numEntries; i++ {
		k := rand.Intn(100000)
		key := []byte(fmt.Sprintf("prefix/key%010d", k))
		val := []byte(fmt.Sprintf("val%010d", k))
		batch.AddEntry(cmn.KV{
			Key:   key,
			Value: val,
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		memTable := NewMemtable(arenaskl.NewArena(1024 * 1024))
		ok, err := memTable.Write(batch, func() error {
			return nil
		})
		require.NoError(b, err)
		require.True(b, ok)
	}
}
