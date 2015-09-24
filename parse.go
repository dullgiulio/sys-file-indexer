// Copyright 2015 Giulio Iotti. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"image"
	"io"
	"log"
	"path/filepath"
	"strconv"
	"time"
)

func parseInt(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	return n
}

func parseHex(dest []byte, s string) {
	d, err := hex.DecodeString(s)
	if err != nil {
		log.Fatal(err)
	}
	copy(dest, d)
}

func loadCSV(fin io.Reader, w *writer) error {
	var buf bytes.Buffer
	r := csv.NewReader(fin)
	defer w.close()
	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		ctime := time.Unix(parseInt(rec[2]), 0)
		p := props{
			fname:   rec[8],
			bname:   rec[13],
			ext:     rec[11],
			dir:     filepath.Dir(rec[8]),
			mime:    rec[12],
			isize:   image.Point{int(parseInt(rec[38])), int(parseInt(rec[39]))},
			size:    parseInt(rec[15]),
			ftype:   int(parseInt(rec[6])),
			modtime: ctime,
			ctime:   ctime,
		}
		parseHex(p.ident[:], rec[9])
		parseHex(p.dident[:], rec[10])
		parseHex(p.chash[:], rec[14])
		p.writeSQL(&buf)
		w.write(buf.String())
		buf.Reset()
	}
	return nil
}

type prefixReader struct {
	scanner *bufio.Scanner
	buf     bytes.Buffer
}

func newPrefixReader(r io.Reader) *prefixReader {
	return &prefixReader{
		scanner: bufio.NewScanner(r),
		buf:     bytes.Buffer{},
	}
}

func (p *prefixReader) Read(buf []byte) (n int, err error) {
	// handle half reads from the buffered data
	n, err = p.buf.Read(buf)
	if n != 0 {
		return
	}
	var hasFile bool
	// read two new lines, file and meta
	for p.scanner.Scan() {
		var readReady bool
		line := p.scanner.Text()
		prefix := line[:5]
		line = line[5:]
		switch prefix {
		case "file:":
			if hasFile {
				return 0, errors.New("invalid line: 'file:' line expected")
			}
			hasFile = true
		case "meta:":
			if !hasFile {
				return 0, errors.New("invalid line: 'meta:' line expected")
			}
			readReady = true
			p.buf.WriteByte(',') // append 'meta:' to the last 'file:' line
		default:
			return 0, errors.New("invalid line: must start with 'meta:' or 'file:'")
		}
		p.buf.WriteString(line)
		if !readReady {
			continue
		}
		p.buf.WriteByte('\n')
		n, _ = p.buf.Read(buf)
		return n, p.scanner.Err()
	}
	return 0, io.EOF
}
