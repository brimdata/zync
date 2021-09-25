package sync

import (
	"errors"
	"flag"

	"github.com/brimdata/zed/pkg/charm"
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
}

func NewFrom(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &From{Sync: parent.(*Sync)}
	return c, nil
}

func (f *From) Run(args []string) error {
	return errors.New("TBD")
}
