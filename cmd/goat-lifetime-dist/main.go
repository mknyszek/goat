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
	"path/filepath"
	"sync"

	"github.com/mknyszek/goat"
	"github.com/mknyszek/goat/cmd/internal/spinner"

	"golang.org/x/exp/mmap"
)

var (
	outputFile   string
	samplePeriod uint
	shardTicks   uint64
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(flag.CommandLine.Output(), "Utility that generates an allocation lifetime\n")
		fmt.Fprintf(flag.CommandLine.Output(), "distribution from an allocation trace.\n")
		fmt.Fprintf(flag.CommandLine.Output(), "usage: %s [flags] <allocation-trace-file>\n")
		flag.PrintDefaults()
	}
	flag.StringVar(&outputFile, "o", "./out.csv", "location to write output files")
	flag.UintVar(&samplePeriod, "sample-period", 1024, "sample every nth allocation")
}

func checkFlags() error {
	if flag.NArg() != 1 {
		return errors.New("incorrect number of arguments")
	}
	if samplePeriod == 0 {
		samplePeriod = 1
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

	var pMu sync.Mutex
	spinner.Start(func() float64 {
		pMu.Lock()
		prog := p.Progress()
		pMu.Unlock()
		return prog
	}, spinner.Format("Processing... %.4f%%"))

	// Map of allocation addresses to the GC in which they were allocated.
	allocs := make(map[uint64]uint32)
	curGC := uint32(0)
	gcActive := false
	allocCount := uint64(0)
	var lifetimes, bLifetimes SmallUint32Hist
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
			if samplePeriod < 2 || allocCount%uint64(samplePeriod) == 0 {
				a := uint32(0)
				if gcActive {
					a = 1 << 31
				}
				allocs[ev.Address] = a | curGC
			}
			allocCount++
		case goat.EventFree:
			if data, ok := allocs[ev.Address]; ok {
				allocGCActive := data&(1<<31) != 0
				allocGC := data &^ (1 << 31)
				bin := curGC - allocGC
				lifetimes.Add(bin)
				if allocGCActive {
					bLifetimes.Add(bin)
				}
				delete(allocs, ev.Address)
			}
		case goat.EventGCStart:
			gcActive = true
		case goat.EventGCEnd:
			gcActive = false
			curGC++
		default:
		}
	}
	spinner.Stop()

	fmt.Println("Writing distribution...")

	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer f.Close()
	lf := lifetimes.Snapshot()
	blf := bLifetimes.Snapshot()
	fmt.Fprintf(f, "# GeneratedFrom: %s\n", filepath.Base(flag.Arg(0)))
	fmt.Fprintf(f, "# RealAllocCount: %d\n", allocCount)
	fmt.Fprintf(f, "# SamplePeriod: %d\n", samplePeriod)
	fmt.Fprintf(f, "GCs,Count,BlackCount\n")
	for i := range lf {
		bc := uint64(0)
		if i < len(blf) {
			bc = blf[i]
		}
		fmt.Fprintf(f, "%d,%d,%d\n", i, lf[i], bc)
	}
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
