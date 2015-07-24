package main

import (
	"flag"
	"path/filepath"
	"runtime"
)

func init() {
	// XXX: Until Go 1.5
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	flag.Parse()
	root := filepath.Clean(filepath.ToSlash(flag.Arg(0)))
	sink := make(chan file)

	// Number of processor workers to process the files
	nproc := runtime.NumCPU()
	// Start all processors
	proc := newProcessor(sink, nproc)
	proc.run()

	// Start scanning the directory
	idx := newIndexer()
	idx.scan(root, sink)

	// Wait for all processors to finish processing files
	proc.wait()
}
