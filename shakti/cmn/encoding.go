package cmn

import "github.com/squareup/pranadb/common"

func EncodeKeyPrefix(buff []byte, dbID uint64, tableID uint64, partitionID uint64) []byte {
	buff = common.AppendUint64ToBufferBE(buff, dbID)
	buff = common.AppendUint64ToBufferBE(buff, tableID)
	buff = common.AppendUint64ToBufferBE(buff, partitionID)
	return buff
}
