// Copyright 2015 Giulio Iotti. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"io"
	"log"
	"os"
	"path"
)

func sumBytes(bs []byte) int {
	var t int
	for _, b := range bs {
		t += int(b)
	}
	return t
}

type file struct {
	os.FileInfo
	path string
	base string
}

func makeFile(f os.FileInfo, name string) file {
	return file{f, path.Dir(name), path.Base(name)}
}

func (f *file) name() string {
	return path.Join(f.path, f.base)
}

type indexer struct {
	stash chan string
	dirs  chan string
	wn    chan int
	out   chan file
	ws    int
	wi    int
}

func newIndexer(ws, wi int) *indexer {
	return &indexer{
		// dirs scheduled to be scanned.
		stash: make(chan string),
		// dirs is for directories to scan from dispatcher to workers.
		dirs: make(chan string),
		// wn is for workers status.
		wn: make(chan int),
		// out is for all found files.
		out: make(chan file),
		// number of workers
		ws: ws,
		// unmber of this worker
		wi: wi,
	}
}

func (i *indexer) canProcess(f file) bool {
	if i.ws < 2 {
		return true
	}
	return sumBytes([]byte(f.name()))%i.ws == i.wi
}

func (i *indexer) sink() <-chan file {
	return i.out
}

func (s *indexer) scan(root string, n int) {
	for i := 0; i < n; i++ {
		go s.worker(i)
	}
	// One worker will pick this up and stop until dispatch starts.
	s.dirs <- root
	s.dispatch(n)
}

func (i *indexer) worker(n int) {
	for dir := range i.dirs {
		i.wn <- n
		i.readdir(dir)
		i.wn <- n
	}
}

func (s *indexer) dispatch(n int) {
	workerActive := make(map[int]bool)
	dirs := make([]string, 0)
	for {
		select {
		// Append a directory to scan.
		case dir := <-s.stash:
			dirs = append(dirs, dir)
		// A worker has started or stopped working.
		case m := <-s.wn:
			// Flip this worker between active and inactive
			workerActive[m] = !workerActive[m]
		}
		freeWorkers := 0
		for i := 0; i < n; i++ {
			// Send one dir out per free worker.
			// If no worker is free, send nothing and
			// keep collecting dirs until a worker is free.
			if !workerActive[i] {
				freeWorkers++
			}
		}
		// If there are more directories to scan,
		// send out the first one and one worker will
		// pick it up.  The dirs chan shouldn't be buffered.
		if len(dirs) == 0 && freeWorkers == n {
			break
		}
		if len(dirs) == 0 {
			continue
		}
		for i := 0; i < freeWorkers; i++ {
			var dir string
			dir, dirs = dirs[0], dirs[1:]
			s.dirs <- dir
			for m := range s.wn {
				workerActive[m] = !workerActive[m]
				if workerActive[m] {
					break
				}
			}
			if len(dirs) == 0 {
				break
			}
		}
		// If there are no pending dirs to scan,
		// and no worker is still active, we have finished.
		var cont bool
		for i := 0; i < n; i++ {
			if workerActive[i] {
				cont = true
				break
			}
		}
		if !cont {
			break
		}
	}
	close(s.stash)
	close(s.dirs)
	close(s.out)
	close(s.wn)
}

func (i *indexer) readdir(dirname string) {
	dir, err := os.Open(dirname)
	if err != nil {
		log.Print(err)
		return
	}
	defer dir.Close()
	for {
		names, err := dir.Readdirnames(1024)
		if err != nil {
			if err != io.EOF {
				log.Print(err)
			}
			return
		}
		for _, name := range names {
			name = path.Join(dirname, name)
			finfo, err := os.Lstat(name)
			if err != nil {
				log.Print(err)
				continue
			}
			f := makeFile(finfo, name)
			// For synlinks, update the finfo with that of the file
			// pointed by the symlink, but keep the link name.
			if f.Mode()&os.ModeSymlink == os.ModeSymlink {
				finfo, err = os.Stat(name)
				if err != nil {
					log.Print(err)
					continue
				}
				f.FileInfo = finfo
			}
			// Subdirectories are queued for scanning
			if f.IsDir() {
				i.stash <- f.name()
				continue
			}
			// Regular files are queued for processing
			if f.Mode().IsRegular() {
				if i.canProcess(f) {
					i.out <- f
				}
			}
		}
	}
}
