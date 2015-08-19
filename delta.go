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
			return fmt.Errorf("Invalid file line: %s", fline)
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
			return fmt.Errorf("Expected at least 18 fields, got %d", len(fields))
		}
		hash, err := hex.DecodeString(fields[9])
		if err != nil {
			return fmt.Errorf("%s: %s", fields[9], err)
		}
		mtime, err := strconv.ParseInt(fields[17], 10, 64)
		if err != nil {
			return fmt.Errorf("Cannot parse modification time: %s", err)
		}
		// Read the next line that contains the "meta:" data
		if !scanner.Scan() {
			return fmt.Errorf("Expected a meta: line, got error: %s", scanner.Err())
		}
		mline := scanner.Text()
		if !strings.HasPrefix(mline, "meta:") {
			return fmt.Errorf("Invalid meta line: %s", mline)
		}
		var key digest
		copy(key[:], hash)
		d[key] = &entry{
			mtime: mtime,
			file:  fline,
			meta:  mline,
		}
	}
	return scanner.Err()
}