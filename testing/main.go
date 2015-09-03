// Copyright 2015 Giulio Iotti. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"encoding/csv"
	"encoding/hex"
	"flag"
	"io"
	"log"
	"os"
)

type entry []string
type entries struct {
	e map[byte][]entry
}

func newEntries() *entries {
	return &entries{
		e: make(map[byte][]entry),
	}
}

func (e *entries) loadCSV(f io.Reader) {
	r := csv.NewReader(f)
	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		if len(record) < 15 {
			log.Fatal("Invalid number of fields")
		}
		hash, err := hex.DecodeString(record[11][0:2])
		if err != nil {
			log.Fatal("Invalid hex string: ", record[11])
		}
		b := hash[0]
		if _, ok := e.e[b]; !ok {
			e.e[b] = make([]entry, 0)
		}
		e.e[b] = append(e.e[b], entry(record))
	}
}

func diff(l, r *entries) bool {
	if len(l.e) != len(r.e) {
		log.Print("Different number of hashes")
	}
	for k := range l.e {
		if _, ok := r.e[k]; !ok {
			log.Printf("Hash %x not present in right", k)
			return false
		}
		if len(l.e[k]) != len(r.e[k]) {
			log.Printf("Different number of entries for hash %x", k)
			return false
		}
		for i := range r.e[k] {
			if len(l.e[k][i]) != len(r.e[k][i]) {
				log.Printf("Different number of fields in record for hash %x", k)
				return false
			}
			left := l.e[k][i]
			right := r.e[k][i]
			for i := 0; i < len(left); i++ {
				if left[i] != right[i] {
					log.Print("Record differ:")
					log.Print(left[i])
					log.Print(right[i])
					return false
				}
			}
		}
	}
	return true
}

func main() {
	flag.Parse()
	f, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Fatal("Invalid left file")
	}
	left := newEntries()
	left.loadCSV(f)
	f.Close()
	f, err = os.Open(flag.Arg(1))
	if err != nil {
		log.Fatal("Invalid right file")
	}
	right := newEntries()
	right.loadCSV(f)
	f.Close()
	diff(left, right)
}
