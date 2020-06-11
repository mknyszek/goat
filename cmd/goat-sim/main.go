package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/mknyszek/goat"
	"github.com/mknyszek/goat/cmd/internal/spinner"
	"github.com/mknyszek/goat/simulation"
	"github.com/mknyszek/goat/simulation/toolbox"
	"github.com/mknyszek/goat/simulation/toolbox/object"
	"github.com/mknyszek/goat/simulation/toolbox/page"
	"github.com/mknyszek/goat/simulation/toolbox/stack"

	"golang.org/x/exp/mmap"
)

var simType string
var period uint64
var outFile string
var implFile string
var sim simulation.Simulator

func go115Simulator() *toolbox.Simulator {
	as := toolbox.NewAddressSpace48(4096)
	pa := page.NewGo114(as)
	sa := stack.NewGo114(pa)
	oa := object.NewGo115(pa)
	return toolbox.NewSimulator(oa, sa)
}

func go115ImmixSimulator() *toolbox.Simulator {
	as := toolbox.NewAddressSpace48(4096)
	pa := page.NewGo114(as)
	sa := stack.NewGo114(pa)
	oa := object.NewImmix(pa)
	return toolbox.NewSimulator(oa, sa)
}

var simulations = map[string]simulation.Simulator{
	"go115":       go115Simulator(),
	"go115+immix": go115ImmixSimulator(),
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(flag.CommandLine.Output(), "Utility that runs an allocation simulation\n")
		fmt.Fprintf(flag.CommandLine.Output(), "and generates a CSV of memory statistics.\n")
		fmt.Fprintf(flag.CommandLine.Output(), "usage: %s [flags] <allocation-trace-file>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.StringVar(&simType, "type", "", "the type of simulation")
	flag.StringVar(&outFile, "o", "./out.csv", "output file for the simulation data")
	flag.StringVar(&implFile, "oimpl", "./out-impl.csv", "output file for implementation-specific simulation data")
	flag.Uint64Var(&period, "period", 2000000000, "the period in CPU ticks to capture stats")
}

func checkFlags() error {
	if flag.NArg() != 1 {
		return errors.New("incorrect number of arguments")
	}
	var sims []string
	for typ, s := range simulations {
		if simType == typ {
			sim = s
		}
		sims = append(sims, typ)
	}
	if sim == nil {
		return fmt.Errorf("-type must be a valid simulation type: %s", strings.Join(sims, ", "))
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

	out, err := os.Create(outFile)
	if err != nil {
		return fmt.Errorf("creating simulation data file: %v", err)
	}
	defer out.Close()

	outImpl, err := os.Create(implFile)
	if err != nil {
		return fmt.Errorf("creating impl-specific simulation data file: %v", err)
	}
	defer outImpl.Close()

	stats := simulation.NewStats()
	sim.RegisterStats(stats)

	fmt.Fprintln(out, "Timestamp,GCCycles,Allocs,Frees,ObjectBytes,StackBytes,UnusedBytes,FreeBytes")
	fmt.Fprintf(outImpl, "Timestamp")
	for _, name := range stats.OtherStats() {
		fmt.Fprintf(outImpl, ",%s", name)
	}
	fmt.Fprintln(outImpl)

	var pMu sync.Mutex
	spinner.Start(func() float64 {
		pMu.Lock()
		prog := p.Progress()
		pMu.Unlock()
		return prog
	}, spinner.Format("Processing... %.4f%%"))

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
		sim.Process(ev, stats)
		diff := stats.Timestamp - ts
		if diff > period {
			// Generate standard stats line.
			fmt.Fprintf(out, "%d,%d,%d,%d,%d,%d,%d,%d\n", stats.Timestamp, stats.GCCycles, stats.Allocs, stats.Frees, stats.ObjectBytes, stats.StackBytes, stats.UnusedBytes, stats.FreeBytes)
			out.Sync()

			// Generate impl-specific stats line.
			fmt.Fprintf(outImpl, "%d", stats.Timestamp)
			for _, name := range stats.OtherStats() {
				fmt.Fprintf(outImpl, ",%d", stats.GetOther(name))
			}
			fmt.Fprintln(outImpl)
			outImpl.Sync()

			ts = stats.Timestamp
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
