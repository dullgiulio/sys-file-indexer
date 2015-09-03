// Copyright 2015 Giulio Iotti. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"strings"
)

type writer struct {
	w    io.Writer
	ch   chan string
	done chan struct{}
}

func newWriter(w io.Writer) *writer {
	return &writer{
		w: w,
		// It's a good idea to buffer this, esp. in SQL mode.
		ch:   make(chan string, 16),
		done: make(chan struct{}),
	}
}

func (w *writer) write(s string) {
	w.ch <- s
}

func (w *writer) close() {
	close(w.ch)
}

func (w *writer) run() {
	var uid int64
	for s := range w.ch {
		uid++
		// In SQL mode we need UID to be set; see comment below on how
		// this is operation is safe.
		if *sqlMode {
			s = strings.Replace(s, "UID", fmt.Sprintf("%d", uid), 1)
		}
		if _, err := io.WriteString(w.w, s); err != nil {
			log.Print("Write to result: ", err)
			break
		}
	}
	w.done <- struct{}{}
}

func (w *writer) wait() {
	<-w.done
}

type splitWriter struct {
	prefix string
	reader io.Reader
	uids   int
}

func (s splitWriter) write(w io.Writer) error {
	var uid int64
	scanner := bufio.NewScanner(s.reader)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, s.prefix) {
			// Replacing the first occurrence of UID is safe here because file
			// contains it as first field, meta as last but has only numeric fields.
			line = strings.Replace(line, "UID", fmt.Sprintf("%d", uid), s.uids)
			if _, err := fmt.Fprintln(w, line[len(s.prefix):]); err != nil {
				return err
			}
			uid++
		}
	}
	return scanner.Err()
}
