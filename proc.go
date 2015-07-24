package main

import (
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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type processor struct {
	nproc int
	in    <-chan file
	hash  chan hash.Hash
	wg    sync.WaitGroup
}

func newProcessor(in <-chan file, n int) *processor {
	p := &processor{
		nproc: n,
		in:    in,
		hash:  make(chan hash.Hash, n),
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
		props := makeProps(h, f)

		// XXX: debug
		log.Print(props.String())

		// TODO:XXX: Send to DB writer

		h.Reset()
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
}

type sha1hash []byte

func filehash(name string, h hash.Hash, r io.Reader) sha1hash {
	if _, err := io.Copy(h, r); err != nil {
		log.Print(name, ": Copy to SHA1: ", err)
		return nil
	}
	return h.Sum(nil)
}

// Metadata to save about a file
type props struct {
	// SHA1 hash of file contents
	ident sha1hash
	// SHA1 hash of directory (XXX: how exactly?)
	dident sha1hash
	// Full filename
	name string
	// Extension
	ext string
	// MIME type
	mime string
	// If image, width x height
	isize image.Point
	// File size in bytes
	size int64
	// Modification time
	modtime time.Time
}

func makeProps(h hash.Hash, f file) props {
	name := f.name()
	ext := filepath.Ext(name)
	p := props{
		size:    f.Size(),
		modtime: f.ModTime(),
		name:    name,
		ext:     ext,
		mime:    mime.TypeByExtension(ext),
	}
	r, err := os.Open(name)
	if err != nil {
		log.Print(name, ": Open: ", err)
		return p
	}
	defer r.Close()
	p.ident = filehash(name, h, r)
	// Non-images are completely processed.
	if !strings.HasPrefix(p.mime, "image/") {
		return p
	}
	// Image-specific processing
	if _, err := r.Seek(0, 0); err != nil {
		log.Print(name, ": Seek: ", err)
		return p
	}
	img, _, err := image.Decode(r)
	if err != nil {
		log.Print(name, ": Decoder: ", err)
		return p
	}
	p.isize = img.Bounds().Size()
	return p
}

func (p props) String() string {
	if strings.HasPrefix(p.mime, "image/") {
		return fmt.Sprintf("%x %dx%d %s", p.ident, p.isize.X, p.isize.Y, p.name)
	}
	return fmt.Sprintf("%x %s", p.ident, p.name)
}
