package zinger

import (
	"fmt"
	"net/http"
	"os"

	"github.com/mccanne/zq/pkg/zio/detector"
)

func handle(format string, producer *Producer, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "bad method", http.StatusForbidden)
		return
	}
	// XXX could encode the kafka topic
	//dirPath, err := parsePath(r.URL.RequestURI())
	//if err != nil {
	//	http.Error(w, err.Error(), http.StatusForbidden)
	//	return
	//}
	reader := detector.LookupReader(format, r.Body, producer.Registry.Resolver)
	if reader == nil {
		panic("couldn't allocate reader: " + format)
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
		err = producer.Write(rec)
		if err != nil {
			// XXX should send more reasonable status code
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func InitServer(port string, producer *Producer) {
	http.HandleFunc("/tsv", func(w http.ResponseWriter, r *http.Request) {
		handle("zeek", producer, w, r)
	})
	http.HandleFunc("/bzng", func(w http.ResponseWriter, r *http.Request) {
		handle("bzng", producer, w, r)
	})
	// http.HandleFunc("/bzng", handleBzng)
	if err := http.ListenAndServe(port, nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}
