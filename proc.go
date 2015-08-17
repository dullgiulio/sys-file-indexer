package main

import (
	"bytes"
	"crypto/sha1"
	"fmt"
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
("%d","0","%d","0","0","1","%d","0","%s","%x","%x","%s","%s","%s","%x","%d","%d","%d");
`
const queryMeta = `INSERT INTO sys_file_metadata (tstamp, crdate, file, width, height) VALUES
("%d","%d","%d","%d","%d");
`

type processor struct {
	nproc  int
	in     <-chan file
	hash   chan hash.Hash
	wg     sync.WaitGroup
	writer *writer
}

func newProcessor(in <-chan file, w *writer, n int) *processor {
	p := &processor{
		nproc:  n,
		in:     in,
		hash:   make(chan hash.Hash, n),
		writer: w,
	}
	for i := 0; i < n; i++ {
		p.hash <- sha1.New()
	}
	return p
}

func (p *processor) runOne() {
	defer p.wg.Done()
	for f := range p.in {
		h := <-p.hash
		p.writer.write(makeProps(h, f))
		p.hash <- h
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

type sha1hash []byte

func strhash(data string, h hash.Hash) sha1hash {
	defer h.Reset()
	if _, err := io.WriteString(h, data); err != nil {
		log.Print("String write to SHA1: ", err)
		return nil
	}
	return h.Sum(nil)
}

func filehash(name string, h hash.Hash, r io.Reader) sha1hash {
	defer h.Reset()
	if _, err := io.Copy(h, r); err != nil {
		log.Print(name, ": Copy to SHA1: ", err)
		return nil
	}
	return h.Sum(nil)
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
	ident sha1hash
	// SHA1 hash of directory
	dident sha1hash
	// SHA1 hash of file contents
	chash sha1hash
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
		mime = mime[:n-1]
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

func makeProps(h hash.Hash, f file) props {
	name := f.name()
	fname := stripRoot(name)
	ext := filepath.Ext(fname)
	dir := filepath.Dir(fname)
	p := props{
		size:    f.Size(),
		modtime: f.ModTime(),
		bname:   filepath.Base(fname),
		fname:   fname,
		ext:     ext,
		dir:     dir,
		mime:    mime.TypeByExtension(ext),
		ctime:   time.Now(),
	}
	r, err := os.Open(name)
	if err != nil {
		log.Print(name, ": Props: ", err)
		return p
	}
	defer r.Close()
	p.ftype = mapType(p.mime)
	p.chash = filehash(name, h, r)
	p.ident = strhash(fname, h)
	p.dident = strhash(dir, h)
	// If the extension is empty, we need to detect
	// the MIME type via file contents
	if p.mime == "" {
		p.mime = sniffMIME(name, r)
	}
	// Non-images are completely processed at this point
	if !strings.HasPrefix(p.mime, "image/") {
		return p
	}
	// Image-specific processing
	if _, err := r.Seek(0, 0); err != nil {
		log.Print(name, ": Seek: ", err)
		return p
	}
	imgconf, _, err := image.DecodeConfig(r)
	if err != nil {
		log.Print(name, ": Image decoder: ", err)
		return p
	}
	p.isize = image.Point{imgconf.Width, imgconf.Height}
	return p
}

func escape(s string) string {
	return strings.Replace(s, `"`, `\"`, -1)
}

// Make sure we have a buffer, we don't want write errors here.
func (p *props) writeBuf(uid uint, w *bytes.Buffer) {
	// Single mode writes a single condensed line.  Used for debugging comparison with tester/tester.
	if *singleMode {
		fmt.Fprintf(w, `"0","0","1","%d","0","%s",`, p.ftype, escape(p.fname))
		fmt.Fprintf(w, `"%x","%x",`, p.ident, p.dident)
		fmt.Fprintf(w, `"%s","%s","%s",`, p.ext, p.mime, escape(p.bname))
		fmt.Fprintf(w, `"%x",`, p.chash)
		fmt.Fprintf(w, "\"%d\",\"%d\",\"%d\"\n", p.size, p.isize.X, p.isize.Y)
		return
	}
	if *sqlMode {
		fmt.Fprintf(w, queryFile, uid, p.ctime.Unix(), p.ftype, escape(p.fname),
			p.ident, p.dident, p.ext, p.mime, escape(p.bname), p.chash, p.size,
			p.ctime.Unix(), p.modtime.Unix())
		fmt.Fprintf(w, queryMeta, p.modtime.Unix(), p.ctime.Unix(), uid, p.isize.X, p.isize.Y)
		return
	}
	fmt.Fprintf(w, `file:"UID","0","%d","0","0","1","%d","0","`, p.ctime.Unix(), p.ftype)
	w.WriteString(escape(p.fname))
	fmt.Fprintf(w, `","%x","%x",`, p.ident, p.dident)
	fmt.Fprintf(w, `"%s","%s","`, p.ext, p.mime)
	w.WriteString(escape(p.bname))
	fmt.Fprintf(w, `","%x","%d",`, p.chash, p.size)
	fmt.Fprintf(w, "\"%d\",\"%d\"\n", p.ctime.Unix(), p.modtime.Unix())
	// Write metadata
	fmt.Fprintf(w, `meta:"0","%d","%d","0","0","0","",`, p.modtime.Unix(), p.ctime.Unix())
	w.WriteString(`"0","0","0","","0","0","0","0","0","0",`)
	fmt.Fprintf(w, `"UID","","%d","%d",`, p.isize.X, p.isize.Y)
	io.WriteString(w, "\"\",\"\",\"0\"\n")
}
