package post

import (
	"flag"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/brimsec/zinger/cmd/zinger/root"
	"github.com/mccanne/charm"
)

var Post = &charm.Spec{
	Name:  "post",
	Usage: "post [options] [file1, file2, ...]",
	Short: "post data to zinger",
	Long: `
The post command allows you to post log files directly to zinger.
If no files are specified, input is taken from stdin allowing you to easily
run a shell pipeline, e.g., using zq, and send the results to kafka via zinger.`,
	New: New,
}

func init() {
	root.Zinger.Add(Post)
}

type Command struct {
	*root.Command
	*http.Client
	zingerAddr string
}

func New(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{
		Command: parent.(*root.Command),
		Client:  &http.Client{},
	}
	f.StringVar(&c.zingerAddr, "a", ":9890", "[addr]:port to send to")
	return c, nil
}

func (c *Command) Run(args []string) error {
	if len(args) == 0 {
		args = []string{"-"}
	}
	for _, fname := range args {
		var f *os.File
		var err error
		if fname == "-" {
			f = os.Stdin
		} else {
			f, err = os.Open(fname)
			if err != nil {
				return err
			}
		}
		_, err = c.post(f)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Command) post(body io.Reader) ([]byte, error) {
	url := "http://" + c.zingerAddr
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	return b, err
}
