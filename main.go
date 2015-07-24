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

func main() {
	flag.Parse()
	root := filepath.Clean(filepath.ToSlash(flag.Arg(0)))
	sink := make(chan file)

	// TODO: From options or default.
	writer := newWriter(os.Stdout)
	go writer.run()

	// Number of processor workers to process the files
	nproc := runtime.NumCPU()
	// Start all processors
	proc := newProcessor(sink, writer, nproc)
	proc.run()

	// Start scanning the directory
	idx := newIndexer()
	idx.scan(sink, root)

	// Wait for all processors to finish processing files.
	// Processors will also wait for writers to finish.
	proc.wait()
}
