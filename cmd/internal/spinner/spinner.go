package spinner

import (
	"fmt"
	"sync"
	"time"
)

// Option is a configuration option for the spinner.
type Option func(cfg *spinnerCfg)

// Format returns a new configuration option for the
// spinner, using the given format string for the spinner.
//
// The string must have exactly one verb in it to support
// a float64 value which is a percent completion.
func Format(ft string) Option {
	return func(cfg *spinnerCfg) {
		cfg.format = ft
	}
}

// Period returns a new configuration option that sets
// the period between screen updates for the spinner.
func Period(p time.Duration) Option {
	return func(cfg *spinnerCfg) {
		cfg.period = p
	}
}

type spinnerCfg struct {
	period time.Duration
	format string
}

var state struct {
	mu      sync.Mutex
	running bool
	done    chan struct{}
}

// Start starts a new global spinner which will write to
// standard output. It uses the function sample to sample
// progress, and sample should return a float64 value between
// 0 and 1 representing a degree of progress.
//
// The default period between updates is 1 second.
//
// Start may not be called again until Stop is called, otherwise
// Start will panic. The spinner is global and therefore only one
// spinner may be active at a time.
func Start(sample func() float64, options ...Option) {
	cfg := spinnerCfg{
		period: time.Second,
		format: "Progress: %.1f%%",
	}
	for _, opt := range options {
		opt(&cfg)
	}
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.running {
		panic("tried to start spinner twice")
	}

	state.running = true
	state.done = make(chan struct{})
	go func() {
		for {
			prog := sample()
			fmt.Printf(cfg.format+"\r", prog*100)
			select {
			case <-state.done:
				fmt.Println()
				close(state.done)
				return
			case <-time.After(cfg.period):
			}
		}
	}()
}

// Stop stops any currently running spinner.
//
// If no spinner is currently running, it does nothing.
func Stop() {
	state.mu.Lock()
	if !state.running {
		state.mu.Unlock()
		return
	}
	done := state.done
	state.mu.Unlock()

	done <- struct{}{}
	<-done

	state.mu.Lock()
	state.running = false
	state.mu.Unlock()
}
