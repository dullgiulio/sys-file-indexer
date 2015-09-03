// Copyright 2015 Giulio Iotti. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import "testing"

func TestMapFtype(t *testing.T) {
	var equivs = []struct {
		mime string
		val  int
	}{
		{"application/vnd.oasis.opendocument.text", 5},
		{"video/x-flv", 4},
		{"audio/x-wav", 3},
		{"image/gif", 2},
		{"text/html", 1},
		{"inode/x-empty", 0},
	}
	for i := range equivs {
		if val := mapType(equivs[i].mime); val != equivs[i].val {
			t.Errorf("Invalid MIME: expected %d got %d for %s", equivs[i].val, val, equivs[i].mime)
		}
	}
}
