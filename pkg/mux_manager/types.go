package mux_manager

import "github.com/soheilhy/cmux"

type Manager interface {
	CreateMux(string, string) (cmux.CMux, error)
	Serve() error
	AssertMux(string, string) (cmux.CMux, error)
	GetMux(string) cmux.CMux
}
