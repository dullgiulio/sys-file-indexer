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

var (
	singleMode = flag.Bool("single", false, "Output in single view mode")
	sqlMode    = flag.Bool("sql", false, "Output in SQL mode")
	fileMode   = flag.String("ofile", "", "Output the CSV for sys_file reading reading from `F`")
	metaMode   = flag.String("ometa", "", "Output the CSV for sys_file_metadata reading from `F`")
	deltaMode  = flag.String("delta", "", "Use commond mode CSV file `F` for cached values")
	profile    = flag.String("profile", "", "Write profiling information to this file `F`")
)

func create(s string) *os.File {
	f, err := os.Create(s)
	if err != nil {
		log.Fatal(err)
	}
	return f
}

func main() {
	flag.Parse()

	// Enable profiling if requested regardless of the
	// mode the tool is run in.
	if *profile != "" {
		pprof.StartCPUProfile(create(*profile))
		defer pprof.StopCPUProfile()
	}

	// Handle the special split modes. In this modes,
	// the user just wants to generate the true CSV to
	// load into the database.
	if *fileMode != "" || *metaMode != "" {
		file := *fileMode
		prefix := "file:"
		if *metaMode != "" {
			file = *metaMode
			prefix = "meta:"
		}
		f, err := os.Open(file)
		if err != nil {
			log.Fatal(err)
		}
		sw := splitWriter{prefix, f}
		if err := sw.write(os.Stdout); err != nil {
			log.Fatal(err)
		}
		return
	}

	delta := makeDelta()

	if *deltaMode != "" {
		f, err := os.Open(*deltaMode)
		if err != nil {
			log.Fatal(err)
		}
		if err := delta.load(f); err != nil {
			log.Fatal(err)
		}
	}

	root := filepath.Clean(filepath.ToSlash(flag.Arg(0)))

	writer := newWriter(os.Stdout)
	go writer.run()

	// Number of processor workers to process the files
	nproc := runtime.NumCPU()

	// Start scanning the directory
	idx := newIndexer()
	go idx.scan(root, nproc)

	// Start all processors
	proc := newProcessor(idx.sink(), writer, nproc, delta)
	proc.run()

	// Wait for all processors to finish processing files.
	// Processors will also wait for writers to finish.
	proc.wait()
}
