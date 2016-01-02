// Copyright 2015 Giulio Iotti. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type deltaFiles []string

func (f *deltaFiles) String() string {
	return strings.Join(*f, "; ")
}

func (f *deltaFiles) Set(value string) error {
	*f = append(*f, value)
	return nil
}

func (f *deltaFiles) IsSet() bool {
	return len(*f) > 0
}

type entry struct {
	mtime      int64
	file, meta string
}

type delta map[digest]*entry

func makeDelta() delta {
	return delta(make(map[digest]*entry))
}

func (d delta) load(r io.Reader) error {
	var buf bytes.Buffer
	scanner := bufio.NewScanner(r)
	parser := csv.NewReader(&buf)
	for scanner.Scan() {
		fline := scanner.Text()
		if !strings.HasPrefix(fline, "file:") {
			return fmt.Errorf("invalid file line: %s", fline)
		}
		// Write this line to a reader for CSV parsing
		fmt.Fprintln(&buf, fline[5:])
		// Parse the last written line
		fields, err := parser.Read()
		if err != nil {
			return err
		}
		// Save memory by keeping the buffer empty
		buf.Reset()
		// Parse filename hash and modification date field
		if len(fields) < 18 {
			return fmt.Errorf("expected at least 18 fields, got %d", len(fields))
		}
		hash, err := hex.DecodeString(fields[9])
		if err != nil {
			return fmt.Errorf("%s: %s", fields[9], err)
		}
		mtime, err := strconv.ParseInt(fields[17], 10, 64)
		if err != nil {
			return fmt.Errorf("cannot parse modification time: %s", err)
		}
		// Read the next line that contains the "meta:" data
		if !scanner.Scan() {
			return fmt.Errorf("expected a meta: line, got error: %s", scanner.Err())
		}
		mline := scanner.Text()
		if !strings.HasPrefix(mline, "meta:") {
			return fmt.Errorf("invalid meta line: %s", mline)
		}
		var key digest
		copy(key[:], hash)
		// If there is already an entry and it has is newer than the one we
		// are trying to insert, do not override the newest entry.
		if e, ok := d[key]; ok {
			if e.mtime >= mtime {
				continue
			}
		}
		d[key] = &entry{
			mtime: mtime,
			file:  fline,
			meta:  mline,
		}
	}
	return scanner.Err()
}

func (d delta) writeTo(w io.Writer) error {
	for _, e := range d {
		if _, err := fmt.Fprintf(w, "file:%s\nmeta:%s\n", e.file, e.meta); err != nil {
			return err
		}
	}
	return nil
}
