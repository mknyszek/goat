# GoAT: Go Allocation Trace

This repository provides libraries and CLI tools for manipulating experimental
Go allocation traces.

## Generating an allocation trace

1. Apply [CL 228817](https://golang.org/cl/228817) to your Go tree.
1. Add calls in your application to export the trace ([API](https://go-review.googlesource.com/c/go/+/228817/12/src/runtime/trace/alloctrace.go#23)).
    * For exporting the trace locally, opening a file and writing to disk
	  is generally sufficient.
1. Run your application with `GOALLOCTRACE=1`.

## Available CLI Tools

* `goat-check`: Sanity checks and optionally prints an allocation trace.

More coming soon.

## Future work

* Add simulation library and tools.
* Add visualization library and tools.
