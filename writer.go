package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"strings"
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

type splitWriter struct {
	prefix string
	reader io.Reader
}

func (s splitWriter) write(w io.Writer) error {
	var uid int64
	scanner := bufio.NewScanner(s.reader)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, s.prefix) {
			line = strings.Replace(line, "UID", fmt.Sprintf("%d", uid), 1)
			fmt.Fprintln(w, line[len(s.prefix):])
			uid++
		}
	}
	return scanner.Err()
}
