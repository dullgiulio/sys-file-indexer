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
	w        io.Writer
	ch       chan string
	done     chan struct{}
	min, inc int
	transf   bool
}

func newWriter(w io.Writer, transform bool, min, inc int) *writer {
	if min < 1 {
		min = 1
	}
	if inc < 1 {
		inc = 1
	}
	return &writer{
		w: w,
		// It's a good idea to buffer this, esp. in SQL mode.
		ch:     make(chan string, 16),
		done:   make(chan struct{}),
		min:    min,
		inc:    inc,
		transf: transform,
	}
}

func (w *writer) write(s string) {
	w.ch <- s
}

func (w *writer) close() {
	close(w.ch)
}

func (w *writer) run() {
	uid := w.min
	for s := range w.ch {
		uid += w.inc
		if w.transf {
			s = strings.Replace(s, "UID", fmt.Sprintf("%d", uid), 2)
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
	min    int
	inc    int
}

func (s splitWriter) write(w io.Writer) error {
	if s.min < 1 {
		s.min = 1
	}
	if s.inc < 1 {
		s.inc = 1
	}
	uid := s.min
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
			uid += s.inc
		}
	}
	return scanner.Err()
}
