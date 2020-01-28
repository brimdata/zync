package zinger

import (
	"log"
	"net/http"

	"github.com/mccanne/zq/zbuf"
	"github.com/mccanne/zq/zio/detector"
	"github.com/mccanne/zq/zng/resolver"
)

func handle(format string, outputs []zbuf.Writer, zctx *resolver.Context, w http.ResponseWriter, r *http.Request) {
	// XXX log new connection
	if r.Method != http.MethodPost {
		http.Error(w, "bad method", http.StatusForbidden)
		return
	}
	var reader zbuf.Reader
	if format == "auto" {
		g := detector.GzipReader(r.Body)
		var err error
		reader, err = detector.NewReader(g, zctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		reader = detector.LookupReader(format, r.Body, zctx)
		if reader == nil {
			log.Panic("couldn't allocate reader: " + format)
		}
	}
	for {
		// XXX might want some sort of batching here, but maybe not.
		rec, err := reader.Read()
		if rec == nil {
			if err != nil {
				// XXX should send more reasonable status code
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		for _, o := range outputs {
			err = o.Write(rec)
			if err != nil {
				// XXX should send more reasonable status code
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}
}

func Run(port string, outputs []zbuf.Writer, zctx *resolver.Context) error {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handle("auto", outputs, zctx, w, r)
	})
	http.HandleFunc("/tsv", func(w http.ResponseWriter, r *http.Request) {
		handle("zeek", outputs, zctx, w, r)
	})
	http.HandleFunc("/bzng", func(w http.ResponseWriter, r *http.Request) {
		handle("bzng", outputs, zctx, w, r)
	})
	return http.ListenAndServe(port, nil)
}
