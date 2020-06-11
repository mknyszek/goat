// Copyright 2020 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/mknyszek/goat"
	"github.com/mknyszek/goat/cmd/internal/spinner"

	"golang.org/x/exp/mmap"
)

var (
	outputFile string
	period     uint64
	cumulative bool
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(flag.CommandLine.Output(), "Utility that generates an allocation size\n")
		fmt.Fprintf(flag.CommandLine.Output(), "distribution from an allocation trace.\n")
		fmt.Fprintf(flag.CommandLine.Output(), "usage: %s [flags] <allocation-trace-file>\n")
		flag.PrintDefaults()
	}
	flag.StringVar(&outputFile, "o", "./size.data", "location to write output file")
	flag.Uint64Var(&period, "period", 2000000000, "the period in CPU ticks to capture a distribution")
	flag.BoolVar(&cumulative, "cum", false, "instead of snapshotting the distribution at a given point in time, accumulate a total distribution")
}

func checkFlags() error {
	if flag.NArg() != 1 {
		return errors.New("incorrect number of arguments")
	}
	return nil
}

func run() error {
	r, err := mmap.Open(flag.Arg(0))
	if err != nil {
		return fmt.Errorf("failed to map trace: %v", err)
	}
	defer r.Close()
	fmt.Println("Generating parser...")
	p, err := goat.NewParser(r)
	if err != nil {
		return fmt.Errorf("creating parser: %v", err)
	}

	out, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("creating data file: %v", err)
	}
	defer out.Close()

	var pMu sync.Mutex
	spinner.Start(func() float64 {
		pMu.Lock()
		prog := p.Progress()
		pMu.Unlock()
		return prog
	}, spinner.Format("Processing... %.4f%%"))

	hist := NewSizeHist()
	sizes := make(map[uint64]uint64)
	var ts uint64
	for {
		pMu.Lock()
		ev, err := p.Next()
		pMu.Unlock()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("parsing events: %v", err)
		}
		switch ev.Kind {
		case goat.EventAlloc:
			hist.Add(ev.Size)
			if cumulative {
				break
			}
			sizes[ev.Address] = ev.Size
		case goat.EventFree:
			if cumulative {
				break
			}
			hist.Sub(sizes[ev.Address])
			delete(sizes, ev.Address)
		}
		diff := ev.Timestamp - ts
		if diff > period {
			// Generate standard stats line.
			fmt.Fprintf(out, ">%d\n", ev.Timestamp)
			hist.ForEach(func(size, count uint64) {
				fmt.Fprintf(out, "%d:%d\n", size, count)
			})
			out.Sync()

			ts = ev.Timestamp
		}
	}
	spinner.Stop()

	return nil
}

func main() {
	flag.Parse()
	if err := checkFlags(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		flag.Usage()
		os.Exit(1)
	}
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}
}
