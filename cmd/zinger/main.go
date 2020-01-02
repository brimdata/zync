package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mccanne/zinger"
)

func parsePath(path string) (string, error) {
	dirpath := filepath.Join(".", path)
	if _, err := os.Stat(dirpath); os.IsNotExist(err) {
		return "", err
	}
	return dirpath, nil
}

func findPath(line string) (string, bool) {
	if strings.HasPrefix(line, "#path") {
		return strings.TrimSpace(line[5:]), true
	}
	return "", false
}

func openfile(path, name string) (*os.File, string, error) {
	for n := 0; n < 100; n++ {
		var fname string
		if n > 0 {
			fname = fmt.Sprintf("%s.%d.log", name, n)
		} else {
			fname = fmt.Sprintf("%s.log", name)
		}
		filename := filepath.Join(path, fname)
		f, err := os.OpenFile(filename, os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			if os.IsExist(err) {
				continue
			}
			return nil, "", err
		}
		return f, filename, nil
	}
	return nil, "", errors.New("too many files")
}

func readPath(reader *bufio.Reader) ([]byte, string, error) {
	var buf []byte
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return nil, "", err
		}
		buf = append(buf, line...)
		if path, ok := findPath(string(line)); ok {
			return buf, path, nil
		}
	}
}

func handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "bad method", http.StatusForbidden)
		return
	}
	dirPath, err := parsePath(r.URL.RequestURI())
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	reader := bufio.NewReader(r.Body)
	header, path, err := readPath(reader)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	file, _, err := openfile(dirPath, path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := file.Write(header); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	go func() {
		for range time.Tick(5 * time.Second) {
			file.Sync()
		}
	}()
	if _, err := io.Copy(file, reader); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func main() {
	port := ":9890"
	if len(os.Args) == 2 {
		port = os.Args[1]
	}
	servers := []string{"localhost:9092"}
	producer, err := zinger.NewProducer(servers, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	zinger.Run(port, producer)
}
