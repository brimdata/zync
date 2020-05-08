package zinger

import (
	"log"
	"net/http"
	"sync/atomic"

	"github.com/brimsec/zq/zbuf"
	"github.com/brimsec/zq/zio/detector"
	"github.com/brimsec/zq/zng/resolver"
	"go.uber.org/zap"
)

func handle(format string, outputs []zbuf.Writer, zctx *resolver.Context, logger *zap.SugaredLogger, w http.ResponseWriter, r *http.Request) {
	logger.Infof("new request: %s %s", r.Method, r.URL)
	defer func() {
		logger.Infof("request complete")
	}()
	if r.Method != http.MethodPost {
		http.Error(w, "bad method", http.StatusForbidden)
		return
	}
	var reader zbuf.Reader
	var err error
	if format == "auto" {
		g := detector.GzipReader(r.Body)
		reader, err = detector.NewReader(g, zctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		reader, err = detector.LookupReader(r.Body, zctx, format)
		if err != nil || reader == nil {
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
				// and if we have multiple outputs, we should
				// still keep going on the one(s) that didn't
				// error rather than bailing here.
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}
}

func Run(port string, outputs []zbuf.Writer, zctx *resolver.Context) error {
	prod, _ := zap.NewProduction()
	logger := prod.Sugar()

	nreqs := int64(0) // there must be a better way to do this with zap...
	logger.Infof("Listening on HTTP %s", port)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		id := atomic.AddInt64(&nreqs, 1)
		l := logger.With("id", id)
		handle("auto", outputs, zctx, l, w, r)
	})
	http.HandleFunc("/tsv", func(w http.ResponseWriter, r *http.Request) {
		id := atomic.AddInt64(&nreqs, 1)
		l := logger.With("id", id)
		handle("zeek", outputs, zctx, l, w, r)
	})
	http.HandleFunc("/bzng", func(w http.ResponseWriter, r *http.Request) {
		id := atomic.AddInt64(&nreqs, 1)
		l := logger.With("id", id)
		handle("bzng", outputs, zctx, l, w, r)
	})
	return http.ListenAndServe(port, nil)
}
