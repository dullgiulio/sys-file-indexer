// Copyright 2015 Giulio Iotti. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	_ "golang.org/x/image/bmp"
	_ "golang.org/x/image/tiff"
	"hash"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const queryFile = `INSERT INTO sys_file (uid, pid, tstamp, last_indexed, missing, storage, type, metadata,
	identifier, identifier_hash, folder_hash, extension, mime_type, name, sha1, size, creation_date, modification_date) VALUES
("UID","0","%d","0","0","1","%d","0","%s","%x","%x","%s","%s","%s","%x","%d","%d","%d");
`
const queryMeta = `INSERT INTO sys_file_metadata (tstamp, crdate, file, width, height) VALUES
("%d","%d","UID","%d","%d");
`

var knownMIME = map[string]string{
	"xls":      "application/vnd.ms-excel",
	"doc":      "application/msword",
	"docx":     "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	"pps":      "application/vnd.ms-powerpoint",
	"ppt":      "application/vnd.ms-powerpoint",
	"pptm":     "application/vnd.openxmlformats-officedocument.presentationml.presentation",
	"ods":      "application/vnd.oasis.opendocument.spreadsheet",
	"odt":      "application/vnd.oasis.opendocument.text",
	"pptx":     "application/vnd.openxmlformats-officedocument.presentationml.presentation",
	"xlsx":     "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	"xlsm":     "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	"docm":     "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	"7z":       "application/x-7z-compressed",
	"exe":      "application/x-dosexec",
	"mm":       "application/x-freemind",
	"mdb":      "application/x-msaccess",
	"rar":      "application/x-rar",
	"swf":      "application/x-shockwave-flash",
	"xml":      "application/xml",
	"wav":      "audio/x-wav",
	"tif":      "image/tiff",
	"bmp":      "image/x-ms-bmp",
	"rtf":      "text/rtf",
	"mp4":      "video/mp4",
	"mpg":      "video/mpeg",
	"mov":      "video/quicktime",
	"flv":      "video/x-flv",
	"wmv":      "video/x-ms-asf",
	"avi":      "video/x-msvideo",
	"htaccess": "text/plain",
}

type processor struct {
	nproc  int
	delta  delta
	writer *writer
	wg     sync.WaitGroup
	in     <-chan file
	tools  chan *tools
}

type tools struct {
	hash hash.Hash
	buf  bytes.Buffer
}

func newProcessor(in <-chan file, w *writer, n int, d delta) *processor {
	p := &processor{
		nproc:  n,
		in:     in,
		writer: w,
		delta:  d,
		tools:  make(chan *tools, n),
	}
	for i := 0; i < n; i++ {
		p.tools <- &tools{sha1.New(), bytes.Buffer{}}
	}
	return p
}

func (p *processor) runOne() {
	defer p.wg.Done()
	useDelta := len(p.delta) > 0
	for f := range p.in {
		var done bool
		// Get one of the available tool structs
		tools := <-p.tools
		// Init basic data for this prop
		name := f.name()
		pr := newProps(tools.hash, f, name)
		// If in delta mode, see if there is a cached delta entry
		if useDelta {
			entry := p.delta[pr.ident]
			// If we have an entry and it's modtime is unchanged, use cached entry
			if entry != nil && f.ModTime().Unix() == entry.mtime {
				p.writer.write(fmt.Sprintf("%s\n%s\n", entry.file, entry.meta))
				done = true
			}
		}
		// Do the normal work to create a new prop then write it
		if !done {
			pr.load(tools.hash, name)
			p.writer.write(pr.marshal(&tools.buf))
		}
		// Free up this tool struct for another worker
		p.tools <- tools
	}
}

func (p *processor) run() {
	p.wg.Add(p.nproc)
	for i := 0; i < p.nproc; i++ {
		go p.runOne()
	}
}

func (p *processor) wait() {
	p.wg.Wait()
	p.writer.close()
	p.writer.wait()
}

type digest [sha1.Size]byte

func strhash(data string, h hash.Hash) []byte {
	defer h.Reset()
	if _, err := io.WriteString(h, data); err != nil {
		log.Print("String write to SHA1: ", err)
		return nil
	}
	return h.Sum(nil)
}

func filehash(name string, h hash.Hash, r io.Reader) []byte {
	defer h.Reset()
	if _, err := io.Copy(h, r); err != nil {
		log.Print(name, ": Copy to SHA1: ", err)
		return nil
	}
	return h.Sum(nil)
}

func guessMIME(ext string) string {
	ext = strings.ToLower(ext)
	if m, ok := knownMIME[ext]; ok {
		return m
	}
	return mime.TypeByExtension(ext)
}

func sniffMIME(name string, r *os.File) string {
	if _, err := r.Seek(0, 0); err != nil {
		log.Print(name, ": Seek: ", err)
		return ""
	}
	buf := make([]byte, 255)
	n, err := r.Read(buf)
	if n == 0 && err != nil {
		log.Print(name, ": Read: ", err)
		return ""
	}
	mimetype := http.DetectContentType(buf)
	n = strings.Index(mimetype, "; ")
	if n >= 0 {
		mimetype = mimetype[:n]
	}
	return mimetype
}

// Metadata to save about a file
type props struct {
	// SHA1 hash of file path + name
	ident digest
	// SHA1 hash of directory
	dident digest
	// SHA1 hash of file contents
	chash digest
	// Full filename
	fname string
	// Basename
	bname string
	// Extension
	ext string
	// Directory name
	dir string
	// MIME type
	mime string
	// If image, width x height
	isize image.Point
	// File size in bytes
	size int64
	// Type of file
	ftype int
	// Modification time
	modtime time.Time
	// Creation time (of this structure)
	ctime time.Time
}

func stripRoot(s string) string {
	n := strings.Index(s, "/")
	if n >= 0 {
		return s[n:]
	}
	return s
}

func mapType(mime string) int {
	n := strings.Index(mime, "/")
	if n >= 0 {
		mime = mime[:n]
	}
	switch mime {
	case "text":
		return 1
	case "image":
		return 2
	case "audio":
		return 3
	case "video":
		return 4
	case "application":
		return 5
	}
	return 0 // unknown
}

func fileExt(fname string) string {
	ext := filepath.Ext(fname)
	if ext != "" {
		ext = ext[1:]
	}
	return ext
}

// Fast operations to fill props struct
func newProps(h hash.Hash, f file, name string) *props {
	fname := stripRoot(name)
	ext := fileExt(fname)
	dir := filepath.Dir(fname)
	ident := strhash(fname, h)
	p := &props{
		modtime: f.ModTime(),
		ctime:   time.Now(),
		fname:   fname,
		ext:     ext,
		dir:     dir,
		size:    f.Size(),
		bname:   filepath.Base(fname),
	}
	copy(p.ident[:], ident)
	return p
}

// Slower operations to fill props struct
func (p *props) load(h hash.Hash, name string) {
	// Empty files always have this special MIME type
	if p.size == 0 {
		p.mime = "inode/x-empty"
	} else {
		p.mime = guessMIME(p.ext)
	}
	r, err := os.Open(name)
	if err != nil {
		log.Print(name, ": Props: ", err)
		return
	}
	defer r.Close()
	// If the extension is empty, we need to detect
	// the MIME type via file contents
	if p.mime == "" {
		p.mime = sniffMIME(name, r)
	}
	p.ftype = mapType(p.mime)
	if _, err := r.Seek(0, 0); err != nil {
		log.Print(name, ": Seek: ", err)
		return
	}
	// TODO: this is quite unreadable
	copy(p.chash[:], filehash(name, h, r))
	copy(p.dident[:], strhash(p.dir, h))
	// Non-images are completely processed at this point
	if !strings.HasPrefix(p.mime, "image/") {
		return
	}
	// Image-specific processing
	if _, err := r.Seek(0, 0); err != nil {
		log.Print(name, ": Seek: ", err)
		return
	}
	imgconf, _, err := image.DecodeConfig(r)
	if err != nil {
		log.Print(name, ": Image decoder: ", err)
		return
	}
	p.isize = image.Point{imgconf.Width, imgconf.Height}
}

func escape(s string) string {
	return strings.Replace(s, `"`, `\"`, -1)
}

func (p *props) marshal(w *bytes.Buffer) string {
	defer w.Reset()
	switch true {
	case *singleMode:
		p.writeSingle(w)
	case *sqlMode:
		p.writeSQL(w)
	default:
		p.writeNormal(w)
	}
	return w.String()
}

// Single mode writes a single condensed line.  Used for debugging comparison with tester/tester.
func (p *props) writeSingle(w io.Writer) {
	fmt.Fprintf(w, `"0","0","1","%d","0","%s",`, p.ftype, escape(p.fname))
	fmt.Fprintf(w, `"%x","%x",`, p.ident, p.dident)
	fmt.Fprintf(w, `"%s","%s","%s",`, p.ext, p.mime, escape(p.bname))
	fmt.Fprintf(w, `"%x",`, p.chash)
	fmt.Fprintf(w, "\"%d\",\"%d\",\"%d\"\n", p.size, p.isize.X, p.isize.Y)
}

func (p *props) writeSQL(w io.Writer) {
	fmt.Fprintf(w, queryFile, p.ctime.Unix(), p.ftype, escape(p.fname),
		p.ident, p.dident, p.ext, p.mime, escape(p.bname), p.chash, p.size,
		p.ctime.Unix(), p.modtime.Unix())
	fmt.Fprintf(w, queryMeta, p.modtime.Unix(), p.ctime.Unix(), p.isize.X, p.isize.Y)
}

func (p *props) writeNormal(w io.Writer) {
	// Write file entry
	fmt.Fprintf(w, `file:"UID","0","%d","0","0","1","%d","0","`, p.ctime.Unix(), p.ftype)
	io.WriteString(w, escape(p.fname))
	fmt.Fprintf(w, `","%x","%x",`, p.ident, p.dident)
	fmt.Fprintf(w, `"%s","%s","`, p.ext, p.mime)
	io.WriteString(w, escape(p.bname))
	fmt.Fprintf(w, `","%x","%d",`, p.chash, p.size)
	fmt.Fprintf(w, "\"%d\",\"%d\"\n", p.ctime.Unix(), p.modtime.Unix())
	// Write metadata
	fmt.Fprintf(w, `meta:"UID","0","%d","%d","0","0","0","",`, p.modtime.Unix(), p.ctime.Unix())
	io.WriteString(w, `"0","0","0","","0","0","0","0","0","0",`)
	fmt.Fprintf(w, `"UID","","%d","%d",`, p.isize.X, p.isize.Y)
	io.WriteString(w, "\"\",\"\",\"0\"\n")
}
