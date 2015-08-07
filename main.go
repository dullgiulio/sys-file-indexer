package main

import (
	"flag"
	"os"
	"path/filepath"
	"runtime"
)

func init() {
	// XXX: Until Go 1.5
	runtime.GOMAXPROCS(runtime.NumCPU())
}

var singleMode = flag.Bool("single", false, "Output in single view mode")

func main() {
	flag.Parse()
	root := filepath.Clean(filepath.ToSlash(flag.Arg(0)))

	// TODO: From options or default.
	writer := newWriter(os.Stdout)
	go writer.run()

	// Number of processor workers to process the files
	nproc := runtime.NumCPU()

	// Start scanning the directory
	idx := newIndexer()
	go idx.scan(root, nproc)

	// Start all processors
	proc := newProcessor(idx.sink(), writer, nproc)
	proc.run()

	// Wait for all processors to finish processing files.
	// Processors will also wait for writers to finish.
	proc.wait()
}
