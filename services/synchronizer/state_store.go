package synchronizer

import (
	"encoding/binary"
	"os"
)

func Uint64ToBytes(n uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(n))

	return b
}

func BytesToUint64(data []byte) uint64 {
	return uint64(binary.LittleEndian.Uint64(data))
}

type StateStore struct {
	StatePath string
	File      *os.File
	Sequence  uint64
}

func CreateStateStore(stateFile string) *StateStore {
	return &StateStore{
		StatePath: stateFile,
		Sequence:  0,
	}
}

func (ss *StateStore) Initialize() error {

	// Open state file
	f, err := os.OpenFile(ss.StatePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	ss.File = f

	// Load state
	ss.Load()

	return err
}

func (ss *StateStore) Load() error {

	var seq uint64
	binary.Read(ss.File, binary.LittleEndian, &seq)

	ss.Sequence = seq

	return nil
}

func (ss *StateStore) Sync() error {

	data := Uint64ToBytes(ss.Sequence)
	_, err := ss.File.WriteAt(data, 0)
	if err != nil {
		return err
	}

	return nil
}
