package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

type file struct {
	os.FileInfo
	path string
}

func makeFile(f os.FileInfo, path string) file {
	return file{f, path}
}

func (f *file) name() string {
	return fmt.Sprintf("%s/%s", f.path, f.FileInfo.Name())
}

type indexer struct {
	wg  sync.WaitGroup
	in  chan string
	out chan file
}

func newIndexer() *indexer {
	return &indexer{
		// Buffering the in channel, directories won't be scanned immediately when found
		// but (probably) after some files have been read.
		in:  make(chan string, 50),
		out: make(chan file),
	}
}

func (i *indexer) addDir(dir string) {
	i.wg.Add(1)
	i.in <- dir
}

func (i *indexer) run() {
	go func() {
		i.wg.Wait()
		close(i.in)
		close(i.out)
	}()
	for dirname := range i.in {
		go i.readdir(dirname)
	}
}

func (i *indexer) readdir(name string) {
	defer i.wg.Done()
	dir, err := os.Open(name)
	if err != nil {
		log.Print(err)
		return
	}
	defer dir.Close()
	for {
		fi, err := dir.Readdir(255)
		if err != nil {
			if err != io.EOF {
				log.Print(err)
			}
			return
		}
		for j := range fi {
			f := makeFile(fi[j], name)
			if f.IsDir() {
				i.addDir(f.name())
				continue
			}
			// TODO: Handling of symlinks outside the root!
			if f.Mode()&os.ModeSymlink != 0 {
				log.Printf("Error: %s skipped. Symlinks are currently not supported!", name)
			}
			if f.Mode().IsRegular() {
				i.out <- f
			}
		}
	}
}

func (i *indexer) sink() <-chan file {
	return i.out
}

func (i *indexer) scan(root string) {
	i.addDir(root)
	go i.run()
}
