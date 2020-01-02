package listen

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/mccanne/charm"
	"github.com/mccanne/zinger"
	"github.com/mccanne/zinger/cmd/zinger/root"
)

var Listen = &charm.Spec{
	Name:  "listen",
	Usage: "listen [options]",
	Short: "listen as a daemon and transcode streaming posts onto kafka",
	Long: `
The listen command allows you to...
TBD.`,
	New: New,
}

func init() {
	root.Zinger.Add(Listen)
}

type Command struct {
	*root.Command
	kafkaCluster    string
	registryCluster string
	listenAddr      string
}

func New(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	f.StringVar(&c.kafkaCluster, "k", "localhost:9092", "ip:port's of one or more kafka servers")
	f.StringVar(&c.registryCluster, "r", "localhost:8081", "ip:port's of one more kafka registry servers")
	f.StringVar(&c.listenAddr, "l", ":9890", "ip:port's of port and interface to listen on")
	return c, nil
}

func parseCluster(s string) []string {
	return strings.Split(s, ",")
}

func (c *Command) Run(args []string) error {
	servers := parseCluster(c.kafkaCluster)
	producer, err := zinger.NewProducer(servers, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return zinger.Run(c.listenAddr, producer)
}

/* this would be good for schema command

func (c *Command) Run(args []string) error {
	if len(args) == 0 {
		return errors.New("no expression to parse")
	}
	expr := strings.Join(args, " ")
	req, err := search.ParseExpr(c.Spacename, expr, c.reverse)
	if err != nil {
		return fmt.Errorf("error compiling search expression: %s\n", err)
	}
	if c.verbose {
		api, err := c.API()
		if err != nil {
			return err
		}
		if req, err = api.PostSearchAST(*req, nil); err != nil {
			return err
		}
	}
	raw, err := json.MarshalIndent(req, "", "    ")
	if err != nil {
		return fmt.Errorf("error compiling search expression: %s\n", err)
	}
	fmt.Println(string(raw))
	return nil
}
*/
