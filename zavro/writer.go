package zavro

import (
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
		w.schemas[id] = GenSchema(r.Descriptor.Type)
	}
	return nil
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
