package zavro

import (
	"crypto/md5"
	"fmt"

	"github.com/brimdata/zed"
	"github.com/go-avro/avro"
)

func EncodeSchema(typ zed.Type, namespace string) (avro.Schema, error) {
	return (&schemaEncoder{namespace, map[*zed.TypeRecord]*avro.RecordSchema{}}).encode(typ)
}

type schemaEncoder struct {
	namespace string
	registry  map[*zed.TypeRecord]*avro.RecordSchema
}

func (s *schemaEncoder) encode(typ zed.Type) (avro.Schema, error) {
	switch typ := zed.AliasOf(typ).(type) {
	case *zed.TypeRecord:
		return s.encodeRecord(typ)
	case *zed.TypeArray:
		return s.encodeArray(typ)
	case *zed.TypeSet:
		return s.encodeSet(typ)
	default:
		return encodeScalarSchema(typ)
	}
}

func (s *schemaEncoder) encodeRecord(typ *zed.TypeRecord) (avro.Schema, error) {
	if actual, ok := s.registry[typ]; ok {
		return &avro.RecursiveSchema{Actual: actual}, nil
	}
	var fields []*avro.SchemaField
	for _, col := range typ.Columns {
		schema, err := s.encode(col.Type)
		if err != nil {
			return nil, err
		}
		if _, ok := schema.(*avro.NullSchema); !ok {
			// Avro unions may not contain more than one unnamed
			// schema with the same type.
			schema = &avro.UnionSchema{
				Types: []avro.Schema{&avro.NullSchema{}, schema},
			}
		}
		fld := &avro.SchemaField{
			Name: col.Name,
			Type: schema,
		}
		fields = append(fields, fld)
	}
	// We hash the zng type to an md5 fingerprint here, otherwise
	// we would get a ton of versions on the same name for different
	// instances/restarts of a zng stream.
	sum := md5.Sum([]byte(typ.String()))
	schema := &avro.RecordSchema{
		Name:       fmt.Sprintf("zng_%x", sum),
		Namespace:  s.namespace,
		Doc:        "Created by zync from zng type " + typ.String(),
		Aliases:    nil,
		Properties: nil,
		Fields:     fields,
	}
	s.registry[typ] = schema
	return schema, nil
}

func (s *schemaEncoder) encodeArray(typ *zed.TypeArray) (avro.Schema, error) {
	inner, err := s.encode(zed.InnerType(typ))
	if err != nil {
		return nil, err
	}
	return &avro.ArraySchema{Items: inner}, nil
}

func (s *schemaEncoder) encodeSet(typ *zed.TypeSet) (avro.Schema, error) {
	// XXX this looks the same as array for now but we will want to add
	// more meta-info to disnguish the two cases
	inner, err := s.encode(zed.InnerType(typ))
	if err != nil {
		return nil, err
	}
	return &avro.ArraySchema{Items: inner}, nil
}

func encodeScalarSchema(typ zed.Type) (avro.Schema, error) {
	switch typ.ID() {
	case zed.IDUint8, zed.IDUint16, zed.IDUint32:
		return &avro.IntSchema{}, nil
	case zed.IDUint64:
		return &avro.LongSchema{}, nil
	case zed.IDInt8, zed.IDInt16, zed.IDInt32:
		return &avro.IntSchema{}, nil
	case zed.IDInt64:
		return &avro.LongSchema{}, nil
	case zed.IDDuration, zed.IDTime:
		return &MicroTimeSchema{}, nil
	case zed.IDFloat32:
		return &avro.FloatSchema{}, nil
	case zed.IDFloat64:
		return &avro.DoubleSchema{}, nil
	case zed.IDBool:
		return &avro.BooleanSchema{}, nil
	case zed.IDBytes:
		return &avro.BytesSchema{}, nil
	case zed.IDString, zed.IDBstring, zed.IDIP, zed.IDNet, zed.IDType, zed.IDError:
		// IP addresses, networks, types, and errors are turned into strings.
		return &avro.StringSchema{}, nil
	case zed.IDNull:
		return &avro.NullSchema{}, nil
	default:
		return nil, fmt.Errorf("encodeScalarSchema: unknown type %q", typ)
	}
}

func DecodeSchema(zctx *zed.Context, schema avro.Schema) (zed.Type, error) {
	switch schema := schema.(type) {
	case *avro.RecordSchema:
		return decodeRecordSchema(zctx, schema)
	case *avro.ArraySchema:
		return decodeArraySchema(zctx, schema)
	case *avro.UnionSchema:
		return decodeUnionSchema(zctx, schema)
	case *avro.RecursiveSchema:
		return decodeRecordSchema(zctx, schema.Actual)
	default:
		return decodeScalarSchema(schema)
	}
}

func decodeRecordSchema(zctx *zed.Context, schema *avro.RecordSchema) (zed.Type, error) {
	cols := make([]zed.Column, 0, len(schema.Fields))
	for _, fld := range schema.Fields {
		fieldType := fld.Type
		// If this field is a union of one type and the null type,
		// then it is an "optional" field in the avro world.  Since
		// all fields in Zed records can be null (aka optional),
		// we'll just smash this to  its underlying type.  The decoder
		// will decode the values against the avro schema and apply
		// the same object to remove the union-wrapper on each said value.
		if opt, _ := isOptional(fieldType); opt != nil {
			fieldType = opt
		}
		typ, err := DecodeSchema(zctx, fieldType)
		if err != nil {
			return nil, err
		}
		cols = append(cols, zed.Column{fld.Name, typ})
	}
	return zctx.LookupTypeRecord(cols)
}

func isOptional(schema avro.Schema) (avro.Schema, int64) {
	if union, ok := schema.(*avro.UnionSchema); ok {
		types := union.Types
		if len(types) == 2 {
			if _, ok := types[0].(*avro.NullSchema); ok {
				return types[1], 0
			}
			if _, ok := types[1].(*avro.NullSchema); ok {
				return types[0], 1
			}
		}
	}
	return nil, -1
}

func decodeArraySchema(zctx *zed.Context, schema *avro.ArraySchema) (zed.Type, error) {
	inner, err := DecodeSchema(zctx, schema.Items)
	if err != nil {
		return nil, err
	}
	return zctx.LookupTypeArray(inner), nil
}

func decodeUnionSchema(zctx *zed.Context, schema *avro.UnionSchema) (zed.Type, error) {
	types := make([]zed.Type, 0, len(schema.Types))
	for _, avroType := range schema.Types {
		typ, err := DecodeSchema(zctx, avroType)
		if err != nil {
			return nil, err
		}
		types = append(types, typ)
	}
	return zctx.LookupTypeUnion(types), nil
}

func decodeScalarSchema(schema avro.Schema) (zed.Type, error) {
	//XXX IPs need metadata/alias, could also try to parse string as option
	//XXX metadata, alias to recover unsigneds?
	switch schema := schema.(type) {
	case *avro.NullSchema:
		return zed.TypeNull, nil
	case *avro.BooleanSchema:
		return zed.TypeBool, nil
	case *avro.IntSchema:
		return zed.TypeInt32, nil
	case *avro.LongSchema:
		return zed.TypeInt64, nil
	case *avro.FloatSchema:
		return zed.TypeFloat32, nil
	case *avro.DoubleSchema:
		return zed.TypeFloat64, nil
	case *avro.BytesSchema:
		return zed.TypeBytes, nil
	case *avro.StringSchema:
		return zed.TypeString, nil
	default:
		return nil, fmt.Errorf("unsupported avro schema type: %T", schema)
	}
}
