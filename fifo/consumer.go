package fifo

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/zcode"
	"github.com/brimdata/zed/zio"
	"github.com/brimdata/zed/zson"
	"github.com/brimdata/zync/connectjson"
	"github.com/brimdata/zync/zavro"
	"github.com/riferrei/srclient"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Consumer struct {
	zctx        *zed.Context
	decoder     decoder
	kclient     *kgo.Client
	savedOffset int64
	topic       string
	metaType    zed.Type
	types       map[zed.Type]map[zed.Type]zed.Type

	recordIter kgo.FetchesRecordIter
}

// decoder wraps the Decode method.
//
// Implementations retain ownership of val and val.Bytes, which remain valid
// until the next Decode..
type decoder interface {
	Decode(b []byte) (val *zed.Value, err error)
}

func NewConsumer(zctx *zed.Context, opts []kgo.Opt, reg *srclient.SchemaRegistryClient, format, topic string, startAt int64, meta bool) (*Consumer, error) {
	var decoder decoder
	switch format {
	case "avro":
		decoder = zavro.NewDecoder(reg, zctx)
	case "json":
		decoder = connectjson.NewDecoder(zctx)
	default:
		return nil, fmt.Errorf("unknonwn format %q", format)
	}
	var metaType zed.Type
	if meta {
		var err error
		metaType, err = zson.ParseType(zctx, "{topic:string,partition:int64,offset:int64}")
		if err != nil {
			return nil, err
		}
	}
	opts = append([]kgo.Opt{
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().At(startAt)),
	}, opts...)
	kclient, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		zctx:        zctx,
		decoder:     decoder,
		kclient:     kclient,
		savedOffset: startAt,
		topic:       topic,
		metaType:    metaType,
		types:       make(map[zed.Type]map[zed.Type]zed.Type),
	}, nil
}

func (c *Consumer) Close() {
	c.kclient.Close()
}

// ReadValue returns the next value.  Unlike zio.Reader.Read, the caller
// receives ownership of zed.Value.Bytes.
func (c *Consumer) ReadValue(ctx context.Context) (*zed.Value, error) {
	for {
		if !c.recordIter.Done() {
			return c.handle(c.recordIter.Next())
		}
		fetches := c.kclient.PollFetches(ctx)
		for _, e := range fetches.Errors() {
			if e.Topic != "" {
				return nil, fmt.Errorf("topic %s, partition %d: %w", e.Topic, e.Partition, e.Err)
			}
			return nil, e.Err
		}
		c.recordIter = *fetches.RecordIter()
	}
}

func (c *Consumer) Run(ctx context.Context, w zio.Writer, timeout time.Duration) error {
	for {
		timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
		val, err := c.ReadValue(timeoutCtx)
		cancel()
		if err != nil {
			if ctx.Err() == nil && timeoutCtx.Err() == context.DeadlineExceeded {
				return nil
			}
			return err
		}
		if err := w.Write(val); err != nil {
			return err
		}
	}
}

func (c *Consumer) handle(krec *kgo.Record) (*zed.Value, error) {
	if krec.Offset < c.savedOffset {
		return nil, fmt.Errorf("received offset %d is less than saved offset %d", krec.Offset, c.savedOffset)
	}
	var b zcode.Builder
	if c.metaType != nil {
		// kafka:{topic:string,partition:int64,offset:int64}
		b.BeginContainer()
		b.Append([]byte(krec.Topic))
		b.Append(zed.EncodeInt(int64(krec.Partition)))
		b.Append(zed.EncodeInt(krec.Offset))
		b.EndContainer()
	}
	key, err := c.decoder.Decode(krec.Key)
	if err != nil {
		return nil, err
	}
	keyType := key.Type
	b.Append(key.Bytes)
	val, err := c.decoder.Decode(krec.Value)
	if err != nil {
		return nil, err
	}
	b.Append(val.Bytes)
	outerType, err := c.outerType(keyType, val.Type)
	if err != nil {
		return nil, err
	}
	return zed.NewValue(outerType, b.Bytes()), nil
}

func (c *Consumer) outerType(key, val zed.Type) (zed.Type, error) {
	m, ok := c.types[key]
	if !ok {
		c.makeType(key, val)
	} else if typ, ok := m[val]; ok {
		return typ, nil
	} else {
		c.makeType(key, val)
	}
	return c.types[key][val], nil
}

func (c *Consumer) makeType(key, val zed.Type) (*zed.TypeRecord, error) {
	cols := []zed.Column{
		zed.NewColumn("kafka", c.metaType),
		zed.NewColumn("key", key),
		zed.NewColumn("value", val),
	}
	if c.metaType == nil {
		cols = cols[1:]
	}
	typ, err := c.zctx.LookupTypeRecord(cols)
	if err != nil {
		return nil, err
	}
	m, ok := c.types[key]
	if !ok {
		m = make(map[zed.Type]zed.Type)
		c.types[key] = m
	}
	m[val] = typ
	return typ, nil
}

func (c *Consumer) Watermarks(ctx context.Context) (int64, int64, error) {
	client := kadm.NewClient(c.kclient)
	start, err := minOffset(client.ListStartOffsets(ctx, c.topic))
	if err != nil {
		return 0, 0, err
	}
	end, err := maxOffset(client.ListEndOffsets(ctx, c.topic))
	if err != nil {
		return 0, 0, err
	}
	return start, end, nil
}

func maxOffset(offsets kadm.ListedOffsets, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	if err := offsets.Error(); err != nil {
		return 0, err
	}
	max := int64(math.MinInt64)
	offsets.Each(func(l kadm.ListedOffset) {
		if l.Offset > max {
			max = l.Offset
		}
	})
	return max, nil
}

func minOffset(offsets kadm.ListedOffsets, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	if err := offsets.Error(); err != nil {
		return 0, err
	}
	min := int64(math.MaxInt64)
	offsets.Each(func(l kadm.ListedOffset) {
		if l.Offset < min {
			min = l.Offset
		}
	})
	return min, nil
}
