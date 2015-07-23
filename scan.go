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
		in:  make(chan string, 20),
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
			}
			i.out <- f
		}
	}
}

func (i *indexer) scan(root string, sink chan<- file) {
	i.addDir(root)
	go i.run()
	for f := range i.out {
		if f.IsDir() {
			// TODO: It should also sink dirs, but they are
			// 		 not supported by the workers at the moment.
			continue
		}
		sink <- f
	}
	close(sink)
}
