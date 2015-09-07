// Copyright 2015 Giulio Iotti. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"io"
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
	osqlMode   = flag.String("osql", "", "Output SQL parsing common CSV from file `F` or stdin")
	fileMode   = flag.String("ofile", "", "Output the CSV for sys_file reading reading from `F`")
	metaMode   = flag.String("ometa", "", "Output the CSV for sys_file_metadata reading from `F`")
	deltaMode  = flag.String("delta", "", "Use common mode CSV file `F` for cached values")
	profile    = flag.String("profile", "", "Write profiling information to this file `F`")
	multiplier = flag.Int("multi", 3, "Number `N` of workers to run for each CPU")
	workerN    = flag.Int("wg", 1, "Total number `N` of workers")
	workerID   = flag.Int("w", 1, "Number `N` of this specific worker instance")
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

	// Special mode that transforms CSV to SQL.
	if *osqlMode != "" {
		var r io.Reader
		if *osqlMode == "-" {
			r = os.Stdin
		} else {
			fr, err := os.Open(*osqlMode)
			if err != nil {
				log.Fatal(err)
			}
			defer fr.Close()
			r = fr
		}
		pr := newPrefixReader(r)
		writer := newWriter(os.Stdout)
		go writer.run()
		if err := loadCSV(pr, writer); err != nil {
			log.Fatal(err)
		}
		writer.wait()
		return
	}

	if *workerN < 1 {
		log.Fatal("Number of workers should be at least one")
	}

	if *workerID < 1 || *workerID > *workerN {
		log.Fatal("Worker number is not valid: must be between 1 and `-wg N`")
	}

	if *multiplier < 1 {
		*multiplier = 1
	}

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
		uids := 1
		if *metaMode != "" {
			file = *metaMode
			prefix = "meta:"
			uids = 2
		}
		f, err := os.Open(file)
		if err != nil {
			log.Fatal(err)
		}
		sw := splitWriter{prefix, f, uids}
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
	nproc := runtime.NumCPU() * *multiplier

	// Start scanning the directory
	idx := newIndexer(*workerN, *workerID-1)
	go idx.scan(root, nproc)

	// Start all processors
	proc := newProcessor(idx.sink(), writer, nproc, delta)
	proc.run()

	// Wait for all processors to finish processing files.
	// Processors will also wait for writers to finish.
	proc.wait()
}
