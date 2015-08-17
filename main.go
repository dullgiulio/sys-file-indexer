package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
)

func init() {
	// XXX: Until Go 1.5
	runtime.GOMAXPROCS(runtime.NumCPU())
}

var singleMode = flag.Bool("single", false, "Output in single view mode")
var sqlMode = flag.Bool("sql", false, "Output in SQL mode")
var profile = flag.String("profile", "", "Write profiling information to this file")

func main() {
	flag.Parse()

	if *profile != "" {
		f, err := os.Create(*profile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

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
