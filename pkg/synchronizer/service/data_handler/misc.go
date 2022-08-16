package data_handler

import "encoding/binary"

func BytesToUint64(data []byte) uint64 {
	return uint64(binary.BigEndian.Uint64(data))
}
