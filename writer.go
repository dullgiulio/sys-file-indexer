package main

import (
	"bytes"
	"io"
	"log"
)

type writer struct {
	w    io.Writer
	ch   chan props
	done chan struct{}
}

func newWriter(w io.Writer) *writer {
	return &writer{
		w:    w,
		ch:   make(chan props),
		done: make(chan struct{}),
	}
}

func (w *writer) write(p props) {
	w.ch <- p
}

func (w *writer) close() {
	close(w.ch)
}

func (w *writer) run() {
	var (
		uid uint
		buf bytes.Buffer
	)
	for p := range w.ch {
		uid++
		p.writeBuf(uid, &buf)
		if _, err := io.Copy(w.w, &buf); err != nil {
			log.Print("Write to result: ", err)
			break
		}
		buf.Reset()
	}
	w.done <- struct{}{}
}

func (w *writer) wait() {
	<-w.done
}
