package zavro

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/zcode"
	"github.com/go-avro/avro"
)

func Decode(in []byte, schema avro.Schema) (zcode.Bytes, error) {
	var b zcode.Builder
	in, err := decodeAny(&b, in, schema)
	if err != nil {
		return nil, err
	}
	if len(in) != 0 {
		return nil, fmt.Errorf("avro decoder: extra data of length %d", len(in))
	}
	switch schema.(type) {
	case *avro.RecordSchema, *avro.RecursiveSchema:
		return b.Bytes().Body()
	}
	return b.Bytes(), nil
}

func decodeAny(b *zcode.Builder, in []byte, schema avro.Schema) ([]byte, error) {
	switch schema := schema.(type) {
	case *avro.RecordSchema:
		return decodeRecord(b, in, schema)
	case *avro.ArraySchema:
		return decodeArray(b, in, schema)
	case *avro.UnionSchema:
		return decodeUnion(b, in, schema)
	case *avro.RecursiveSchema:
		return decodeRecord(b, in, schema.Actual)
	default:
		return decodeScalar(b, in, schema)
	}
}

func decodeRecord(b *zcode.Builder, in []byte, schema *avro.RecordSchema) ([]byte, error) {
	b.BeginContainer()
	for _, avroField := range schema.Fields {
		var err error
		in, err = decodeAny(b, in, avroField.Type)
		if err != nil {
			return nil, err
		}
	}
	b.EndContainer()
	return in, nil
}

func decodeArray(b *zcode.Builder, in []byte, schema *avro.ArraySchema) ([]byte, error) {
	elemType := schema.Items
	for {
		// XXX check for size exceeded on array that doesn't fit in mem
		var n int64
		in, n = decodeVarint(in)
		if in == nil {
			return nil, errors.New("bad array encoding in avro serialization")
		}
		if n == 0 {
			break
		}
		if n < 0 {
			n = -n
			in, _ = decodeVarint(in)
			if in == nil {
				return nil, errors.New("bad array encoding in avro serialization")
			}
		}
		var err error
		in, err = decodeAny(b, in, elemType)
		if err != nil {
			return nil, err
		}
	}
	return in, nil
}

func decodeUnion(b *zcode.Builder, in []byte, schema *avro.UnionSchema) ([]byte, error) {
	in, selector := decodeVarint(in)
	if in == nil {
		return nil, errors.New("end of input decoding avro union")
	}
	if selector < 0 || int(selector) >= len(schema.Types) {
		return nil, fmt.Errorf("bad selector decoding avro union (%d when len %d)", selector, len(schema.Types))
	}
	if schema, nullSelector := isOptional(schema); schema != nil {
		if selector == nullSelector {
			b.Append(nil)
			return in, nil
		}
		return decodeAny(b, in, schema)
	}
	b.BeginContainer()
	b.Append(zed.EncodeInt(int64(selector)))
	in, err := decodeAny(b, in, schema.Types[selector])
	b.EndContainer()
	return in, err
}

func decodeVarint(in []byte) ([]byte, int64) {
	val, n := binary.Varint(in)
	if n <= 0 {
		return nil, 0
	}
	return in[n:], val
}

func decodeCountedValue(in []byte) ([]byte, []byte) {
	in, n := decodeVarint(in)
	if in == nil || n < 0 || int(n) > len(in) {
		return nil, nil
	}
	return in[n:], in[:n]
}

func decodeScalar(b *zcode.Builder, in []byte, schema avro.Schema) ([]byte, error) {
	switch schema := schema.(type) {
	case *avro.NullSchema:
		b.Append(nil)
		return in, nil
	case *avro.BooleanSchema:
		if len(in) == 0 {
			return nil, errors.New("end of input decoding bool")
		}
		// ZNG and Avro are the same here
		b.Append(in[:1])
		return in[1:], nil
	case *avro.IntSchema, *avro.LongSchema:
		var v int64
		in, v = decodeVarint(in)
		if in == nil {
			return nil, errors.New("error decoding avro long")
		}
		b.Append(zed.EncodeInt(v))
		return in, nil
	case *avro.FloatSchema:
		if len(in) < 4 {
			return nil, errors.New("end of input decoding avro float")
		}
		b.Append(in[:4])
		return in[4:], nil
	case *avro.DoubleSchema:
		if len(in) < 8 {
			return nil, errors.New("end of input decoding avro double")
		}
		b.Append(in[:8])
		return in[8:], nil
	case *avro.BytesSchema, *avro.StringSchema:
		in, body := decodeCountedValue(in)
		if in == nil {
			return nil, errors.New("end of input decoding avro bytes or string")
		}
		b.Append(body)
		return in, nil
	default:
		return nil, fmt.Errorf("unsupported avro schema: %T", schema)
	}
}
