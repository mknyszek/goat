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
	type allocData struct {
		gcData uint32
		size   uint64
	}
	allocs := make(map[uint64]allocData)
	curGC := uint32(0)
	gcActive := false
	var allocCount, freeCount uint64
	var (
		lifetimesByObject, lifetimesByABObject SmallUint32Hist
		lifetimesByBytes, lifetimesByABBytes   SmallUint32Hist
	)
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
				allocs[ev.Address] = allocData{
					gcData: a | curGC,
					size:   ev.Size,
				}
			}
			allocCount++
		case goat.EventFree:
			if data, ok := allocs[ev.Address]; ok {
				allocGCActive := data.gcData&(1<<31) != 0
				allocGC := data.gcData &^ (1 << 31)
				bin := curGC - allocGC
				lifetimesByObject.Add(bin)
				lifetimesByBytes.AddN(bin, data.size)
				if allocGCActive {
					lifetimesByABObject.Add(bin)
					lifetimesByABBytes.AddN(bin, data.size)
				}
				delete(allocs, ev.Address)
			}
			freeCount++
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
	olf := lifetimesByObject.Snapshot()
	abolf := lifetimesByABObject.Snapshot()
	blf := lifetimesByBytes.Snapshot()
	abblf := lifetimesByABBytes.Snapshot()
	fmt.Fprintf(f, "# GeneratedFrom: %s\n", filepath.Base(flag.Arg(0)))
	fmt.Fprintf(f, "# TotalSamplesCount: %d\n", freeCount)
	fmt.Fprintf(f, "# SamplePeriod: %d\n", samplePeriod)
	fmt.Fprintf(f, "GCs,Objects,AllocBlackObjects,Bytes,AllocBlackBytes\n")
	for i := range olf {
		obc := uint64(0)
		if i < len(abolf) {
			obc = abolf[i]
		}
		bbc := uint64(0)
		if i < len(abblf) {
			bbc = abblf[i]
		}
		fmt.Fprintf(f, "%d,%d,%d,%d,%d\n", i, olf[i], obc, blf[i], bbc)
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
