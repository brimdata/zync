package zavro

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/brimdata/zed/zcode"
	"github.com/brimdata/zed/zng"
	"github.com/go-avro/avro"
)

func Decode(in []byte, schema *avro.RecordSchema) (zcode.Bytes, error) {
	var b zcode.Builder
	in, err := decodeRecord(&b, in, schema)
	if err != nil {
		return nil, err
	}
	if len(in) != 0 {
		return nil, fmt.Errorf("avro decoder: extra data of length %d", len(in))
	}
	return b.Bytes().ContainerBody()
}

func decodeAny(b *zcode.Builder, in []byte, schema avro.Schema) ([]byte, error) {
	switch schema := schema.(type) {
	case *avro.RecordSchema:
		return decodeRecord(b, in, schema)
	case *avro.ArraySchema:
		return decodeArray(b, in, schema)
	case *avro.UnionSchema:
		return decodeUnion(b, in, schema)
	default:
		return decodeScalar(b, in, schema)
	}
}

func decodeRecord(b *zcode.Builder, in []byte, schema *avro.RecordSchema) ([]byte, error) {
	b.BeginContainer()
	for _, avroField := range schema.Fields {
		avroType := avroField.Type
		var err error
		in, err = decodeAny(b, in, avroType)
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
		var cnt int64
		in, cnt = decodeVarint(in)
		if in == nil {
			return nil, errors.New("bad array encoding in avro serialization")
		}
		if cnt == 0 {
			break
		}
		if cnt < 0 {
			cnt = -cnt
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
	if schema := isOptional(schema); schema != nil {
		//XXX assume this is an "optional" field as encoded by us
		// and decode the first value of the union as the actual value
		// if present.  XXX We should handle optional-value unions that
		// have null first.
		if selector == 0 {
			b.AppendNull()
			return in, nil
		}
		return decodeAny(b, in, schema)
	}
	b.BeginContainer()
	b.AppendPrimitive(zng.EncodeInt(int64(selector)))
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
	case *avro.BooleanSchema:
		if len(in) == 0 {
			return nil, errors.New("end of input decoding bool")
		}
		// ZNG and Avro are the same here
		b.AppendPrimitive(in[:1])
		return in[1:], nil
	case *avro.LongSchema:
		var v int64
		in, v = decodeVarint(in)
		if in == nil {
			return nil, errors.New("error decoding avro long")
		}
		b.AppendPrimitive(zng.EncodeInt(v))
		return in, nil
	case *avro.FloatSchema, *avro.DoubleSchema: //XXX
		// avro says this is Java's doubleToLongBits...
		// we need to check if Go math lib is the same
		if len(in) < 8 {
			return nil, errors.New("end of input decoding avro float")
		}
		b.AppendPrimitive(in[:8])
		return in[8:], nil
	case *avro.StringSchema:
		in, body := decodeCountedValue(in)
		if in == nil {
			return nil, errors.New("end of input decoding avro string")
		}
		b.AppendPrimitive(body)
		return in, nil
		/*
			case zng.IDTime:
				// XXX map a nano to a microsecond time
				ts, err := zng.DecodeInt(body)
				if err != nil {
					return nil, err
				}
				us := ts / 1000
				return appendVarint(dst, us), nil
		*/
	default:
		return nil, fmt.Errorf("unsupported avro schema: %T", schema)
	}
}
