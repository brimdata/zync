package zinger

import (
	"net/http"

	"github.com/mccanne/zq/pkg/zio/detector"
)

func handle(format string, producer *Producer, w http.ResponseWriter, r *http.Request) {
	// XXX log new connection
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
	//fmt.Println("READ ALL")
	//b, err := ioutil.ReadAll(r.Body)
	//if err != nil {
	//	fmt.Println(err)
	//}
	//fmt.Println(string(b))

	reader := detector.LookupReader(format, r.Body, producer.Resolver)
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

func Run(port string, producer *Producer) error {
	http.HandleFunc("/tsv", func(w http.ResponseWriter, r *http.Request) {
		handle("zeek", producer, w, r)
	})
	http.HandleFunc("/bzng", func(w http.ResponseWriter, r *http.Request) {
		handle("bzng", producer, w, r)
	})
	return http.ListenAndServe(port, nil)
}
