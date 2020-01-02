package zavro

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/mccanne/zq/pkg/nano"
	"github.com/mccanne/zq/pkg/peeker"
	"github.com/mccanne/zq/pkg/zng"
	"github.com/mccanne/zq/pkg/zng/resolver"
)

const (
	ReadSize = 512 * 1024
	MaxSize  = 10 * 1024 * 1024
)

type Reader struct {
	peeker *peeker.Reader
	mapper *resolver.Mapper
}

func NewReader(reader io.Reader, r *resolver.Table) *Reader {
	return &Reader{
		peeker: peeker.NewReader(reader, ReadSize, MaxSize),
		mapper: resolver.NewMapper(r),
	}
}

func (r *Reader) Read() (*zng.Record, error) {
	return nil, errors.New("kavroio.Reader: not yet implemented")
}

func (r *Reader) parseValue(id int, b []byte) (*zng.Record, error) {
	descriptor := r.mapper.Map(id)
	if descriptor == nil {
		return nil, zng.ErrDescriptorInvalid
	}
	//XXX decode avro in b to zval... need avro schema?
	record := zng.NewVolatileRecord(descriptor, nano.MinTs, b)
	if err := record.TypeCheck(); err != nil {
		return nil, err
	}
	//XXX this should go in NewRecord?
	ts, err := record.AccessTime("ts")
	if err == nil {
		record.Ts = ts
	}
	return record, nil
}

func (r *Reader) decodeHeader() (int, error) {
	hdr, err := r.peeker.Read(5)
	if err == io.EOF {
		return 0, err
	}
	if len(hdr) < 5 {
		return 0, errors.New("truncated buffer")
	}
	id := binary.BigEndian.Uint32(hdr[1:])
	return int(id), nil
}
