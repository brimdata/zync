package zavro

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/go-avro/avro"
	"github.com/mccanne/zq/pkg/zng"
	"github.com/mccanne/zq/pkg/zng/resolver"
)

type Writer struct {
	io.Writer
	tracker *resolver.Tracker
	schemas map[int]avro.Schema
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{
		Writer:  w,
		tracker: resolver.NewTracker(),
		schemas: make(map[int]avro.Schema),
	}
}

func (w *Writer) Write(r *zng.Record) error {
	id := r.Descriptor.ID
	if !w.tracker.Seen(id) {
		w.schemas[id] = genSchema(r.Descriptor.Type)
	}
	// build kafka/avro header
	var hdr [5]byte
	hdr[0] = 0
	binary.BigEndian.PutUint32(hdr[1:], uint32(id))
	_, err := w.Writer.Write(hdr[:])
	if err != nil {
		return err
	}
	// write value body seralized as avro
	b, err := encodeRecord(nil, r.Type, r.Raw)
	if err != nil {
		return err
	}
	_, err = w.Writer.Write(b)
	return err
}

// just dump schemas to stdout for debugging
func (w *Writer) Flush() error {
	b, err := json.MarshalIndent(w.schemas, "", "    ")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}
