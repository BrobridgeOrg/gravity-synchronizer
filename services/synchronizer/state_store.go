package synchronizer

import (
	"encoding/binary"
	"os"
)

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

	ss.File.Seek(0, os.SEEK_SET)

	err := binary.Write(ss.File, binary.LittleEndian, ss.Sequence)
	if err != nil {
		return err
	}

	ss.File.Sync()

	return nil
}
