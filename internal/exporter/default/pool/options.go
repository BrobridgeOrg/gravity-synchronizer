package pool

import "time"

type Options struct {
	InitCap     int
	MaxCap      int
	DialTimeout time.Duration
	IdleTimeout time.Duration
}

func NewOptions() *Options {
	return &Options{
		InitCap:     8,
		MaxCap:      128,
		DialTimeout: 10 * time.Second,
		IdleTimeout: 60 * time.Second,
	}
}
