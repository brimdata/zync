package sync

import (
	"context"
	"errors"
	"flag"
	"fmt"

	lakeapi "github.com/brimdata/zed/lake/api"
	"github.com/brimdata/zed/pkg/charm"
	"github.com/brimdata/zed/zson"
	"github.com/brimdata/zinger/cli"
	"github.com/brimdata/zinger/fifo"
	"github.com/riferrei/srclient"
)

var FromSpec = &charm.Spec{
	Name:  "from",
	Usage: "from [options]",
	Short: "sync a Kafka topic to a Zed lake pool",
	Long: `
The from command syncs data from a Zed lake to a Kafka topic acting
as a source of Zed data for Kafka.
The Zed records are transcoded from Avro into Zed and synced
to the target Zed data pool and branch specified by the use flag.
The data pool is expected to have the pool key "kafka.offset" sorted
in descending order.

Each kafka key and value appears as a sub-record in the transcoded Zed record
along with a meta-data record called "kafka", i.e., having this Zed type signature:
{
        kafka:{topic:string,offset:int64,input_offset:int64,partition:int64},
        key:{...},
        value:{...},
}

The kafka.offset field is a transactionally consistent offset in the target
pool where all commits to the pool have consectutive offsets in the same order
as the consumed data.  Multiple kafka queues may be sync'd and mixed together
in the target pool and the merged kafka.offsets are always serially consistent.

The field kafka.input_offset is the original offset from the inbound kafka queue,
allowing the from command to be restartable and crash-recoverable as the largest
kafka.input_offset for any given topic can easily be queried on restart
in a transactionally consistent fashion.
`,
	New: NewFrom,
}

type From struct {
	*Sync
	pool  string
	flags cli.Flags
}

func NewFrom(parent charm.Command, fs *flag.FlagSet) (charm.Command, error) {
	f := &From{Sync: parent.(*Sync)}
	fs.StringVar(&f.pool, "p", "", "name of Zed lake pool")
	f.flags.SetFlags(fs)
	return f, nil
}

//XXX get this working then we need to add Seek() to consumer and start from
// correct position.

func (f *From) Run(args []string) error {
	if f.flags.Topic == "" {
		return errors.New("kafka topic must be specified with -t")
	}
	ctx := context.TODO()
	service, err := lakeapi.OpenRemoteLake(ctx, f.flags.Host)
	if err != nil {
		return err
	}
	lk, err := fifo.NewLake(f.pool, service)
	if err != nil {
		return err
	}
	consumerOffset, err := lk.NextConsumerOffset(f.flags.Topic)
	if err != nil {
		return err
	}
	url, secret, err := cli.SchemaRegistryEndpoint()
	if err != nil {
		return err
	}
	config, err := cli.LoadKafkaConfig()
	if err != nil {
		return err
	}
	registry := srclient.CreateSchemaRegistryClient(url)
	registry.SetCredentials(secret.User, secret.Password)
	zctx := zson.NewContext()
	consumer, err := fifo.NewConsumer(zctx, config, registry, f.flags.Topic, consumerOffset, true)
	if err != nil {
		return err
	}
	from := fifo.NewFrom(zctx, lk, consumer)
	ncommit, nrec, err := from.Sync(ctx)
	if ncommit != 0 {
		fmt.Printf("synchronized %d record%s in %d commit%s\n", nrec, plural(nrec), ncommit, plural(ncommit))
	} else {
		fmt.Println("nothing new found to synchronize")
	}
	//XXX close consumer?
	return err
}

func plural(n int64) string {
	if n == 1 {
		return ""
	}
	return "s"
}
