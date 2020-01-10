package zavro

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/go-avro/avro"
	"github.com/mccanne/zq/pkg/zeek"
	"github.com/mccanne/zq/pkg/zng"
)

func Translate(dst []byte, id uint32, route *Route, r *zng.Record) ([]byte, error) {
	// build kafka/avro header
	var hdr [5]byte
	hdr[0] = 0
	binary.BigEndian.PutUint32(hdr[1:], uint32(id))
	dst = append(dst, hdr[:]...)
	return translateRecord(dst, route, r)
}

func isOptional(schema avro.Schema, inputField, outputField string) error {
	union, ok := schema.(*avro.UnionSchema)
	if ok {
		for _, s := range union.Types {
			if _, ok := s.(*avro.NullSchema); ok {
				return nil
			}
		}
	}
	return fmt.Errorf("%s: non-optional output field has no default for absent input field %s", outputField, inputField)
}

func translateRecord(dst []byte, route *Route, r *zng.Record) ([]byte, error) {
	for k, col := range route.Schema.Fields {
		inputField := route.InputField[k]
		val := r.ValueByField(inputField)
		if val == nil {
			// field isn't here... look for default
			def := col.Default
			if def == nil {
				// if this field is a union type with a null,
				// we're good to go.
				if err := isOptional(col.Type, inputField, col.Name); err != nil {
					return nil, err
				}
				continue
			}
			dst, err := encodeDefault(dst, col.Type, col.Default)
			if err != nil {
				return nil, err
			}
		}
		dst, err := translateValue(dst, col.Type, val)
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func translateValue(dst []byte, schema avro.Schema, value zeek.Value) ([]byte, error) {
	switch typ := schema.(type) {
	default:
		return nil, fmt.Errorf("unsupported avro schema %s", typ.String())

	case *avro.StringSchema:
		b := zeek.Unescape([]byte(value.String()))
		return appendCountedValue(dst, b), nil

	case *avro.BytesSchema:
		return nil, errors.New("avro bytes type not yet supported")

	case *avro.IntSchema, *avro.LongSchema:
		var v zeek.Int
		if !zeek.CoerceToInt(value, &v) {
			return nil, fmt.Errorf("couldn't coerce zeek type %s to int", value.Type().String())
		}
		return appendVarint(dst, int64(v)), nil

	case *avro.FloatSchema:
		return nil, errors.New("avro float type not yet supported")

	case *avro.DoubleSchema:
		// avro says this is Java's doubleToLongBits...
		// we need to check if Go math lib is the same
		var v zeek.Double
		if !zeek.CoerceToDouble(value, &v) {
			return nil, fmt.Errorf("couldn't coerce zeek type %s to double", value.Type().String())
		}
		return append(dst, zeek.EncodeDouble(float64(v))...), nil

	case *avro.BooleanSchema:
		var v bool
		b, ok := value.(*zeek.Bool)
		if ok {
			v = bool(*b)
		} else {
			i, ok := value.(*zeek.Int)
			if !ok {
				return nil, fmt.Errorf("couldn't coerce zeek type %s to bool", value.Type().String())
			}
			if *i != 0 {
				v = true
			}
		}
		if v {
			return append(dst, byte(1)), nil
		}
		return append(dst, byte(0)), nil

	case *avro.NullSchema:
		return dst, nil

	case *avro.RecordSchema:
		return nil, errors.New("nested records not yet supported")

	case *avro.EnumSchema:
		return nil, errors.New("avro enum type not yet supported")

	case *avro.ArraySchema:
	case *avro.MapSchema:
	case *avro.UnionSchema:
		return translateUnion(dst, typ, value)
	case *avro.FixedSchema:
		return nil, errors.New("avro enum type not yet supported")

	}
	return dst, nil
}
