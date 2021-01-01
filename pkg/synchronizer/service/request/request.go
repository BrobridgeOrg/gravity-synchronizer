package request

type Request interface {
	Data() []byte
	Error(error) error
	Respond() error
}
