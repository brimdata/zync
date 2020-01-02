package zinger

import (
	"fmt"
	"net/http"
	"os"

	"github.com/mccanne/zq/pkg/zio/detector"
)

func handle(format string, producer *Producer, w http.ResponseWriter, r *http.Request) {
	fmt.Println("HANDLE")
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
	fmt.Println("READER")
	for {
		// XXX might want some sort of batching here, but maybe not.
		rec, err := reader.Read()
		if rec == nil {
			if err != nil {
				// XXX should send more reasonable status code
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			fmt.Println("EOS")
			return
		}
		fmt.Println("READ ONE")
		err = producer.Write(rec)
		if err != nil {
			// XXX should send more reasonable status code
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func Run(port string, producer *Producer) {
	http.HandleFunc("/tsv", func(w http.ResponseWriter, r *http.Request) {
		handle("zeek", producer, w, r)
	})
	http.HandleFunc("/bzng", func(w http.ResponseWriter, r *http.Request) {
		handle("bzng", producer, w, r)
	})
	// http.HandleFunc("/bzng", handleBzng)
	if err := http.ListenAndServe(port, nil); err != nil {
		//XXX return err
		fmt.Fprintln(os.Stderr, err)
	}
}
