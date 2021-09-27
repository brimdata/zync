package zavro

import (
	"crypto/md5"
	"fmt"

	"github.com/brimdata/zed/zng"
	"github.com/brimdata/zed/zson"
	"github.com/go-avro/avro"
)

func EncodeSchema(typ zng.Type, namespace string) (avro.Schema, error) {
	switch typ := typ.(type) {
	case *zng.TypeRecord:
		return encodeRecordSchema(typ, namespace)
	case *zng.TypeArray:
		return encodeArraySchema(typ, namespace)
	case *zng.TypeSet:
		return encodeSetSchema(typ, namespace)
	default:
		return encodeScalarSchema(typ)
	}
}

func encodeRecordSchema(typ *zng.TypeRecord, namespace string) (avro.Schema, error) {
	var fields []*avro.SchemaField
	for _, col := range typ.Columns {
		schema, err := EncodeSchema(col.Type, namespace)
		if err != nil {
			return nil, err
		}
		fld := &avro.SchemaField{
			Name: col.Name,
			Type: &avro.UnionSchema{
				Types: []avro.Schema{&avro.NullSchema{}, schema},
			},
		}
		fields = append(fields, fld)
	}
	// We hash the zng type to an md5 fingerprint here, otherwise
	// we would get a ton of versions on the same name for different
	// instances/restarts of a zng stream.
	sum := md5.Sum([]byte(typ.String()))
	return &avro.RecordSchema{
		Name:       fmt.Sprintf("zng_%x", sum),
		Namespace:  namespace,
		Doc:        "Created by zinger from zng type " + typ.String(),
		Aliases:    nil,
		Properties: nil,
		Fields:     fields,
	}, nil
}

func encodeArraySchema(typ *zng.TypeArray, namespace string) (avro.Schema, error) {
	inner, err := EncodeSchema(zng.InnerType(typ), namespace)
	if err != nil {
		return nil, err
	}
	return &avro.ArraySchema{Items: inner}, nil
}

func encodeSetSchema(typ *zng.TypeSet, namespace string) (avro.Schema, error) {
	// XXX this looks the same as array for now but we will want to add
	// more meta-info to disnguish the two cases
	inner, err := EncodeSchema(zng.InnerType(typ), namespace)
	if err != nil {
		return nil, err
	}
	return &avro.ArraySchema{Items: inner}, nil
}

func encodeScalarSchema(typ zng.Type) (avro.Schema, error) {
	switch typ.ID() {
	case zng.IDNull:
		return &avro.NullSchema{}, nil
	case zng.IDIP:
		// IP addresses are turned into strings...
		return &avro.StringSchema{}, nil
	case zng.IDBool:
		return &avro.BooleanSchema{}, nil
	case zng.IDInt64:
		return &avro.LongSchema{}, nil
	case zng.IDUint64:
		return &avro.LongSchema{}, nil
	case zng.IDFloat64:
		return &avro.DoubleSchema{}, nil
	case zng.IDFloat32:
		return &avro.FloatSchema{}, nil
	case zng.IDDuration:
		return &MicroTimeSchema{}, nil
	case zng.IDString, zng.IDBstring:
		return &avro.StringSchema{}, nil
	case zng.IDNet:
		return &avro.StringSchema{}, nil
	case zng.IDTime:
		return &MicroTimeSchema{}, nil
	default:
		return nil, fmt.Errorf("encodeScalarSchema: unknown type %q", typ)
	}
}

func DecodeSchema(zctx *zson.Context, schema avro.Schema) (zng.Type, error) {
	switch schema := schema.(type) {
	case *avro.RecordSchema:
		return decodeRecordSchema(zctx, schema)
	case *avro.ArraySchema:
		return decodeArraySchema(zctx, schema)
	case *avro.UnionSchema:
		return decodeUnionSchema(zctx, schema)
	default:
		return decodeScalarSchema(schema)
	}
}

func decodeRecordSchema(zctx *zson.Context, schema *avro.RecordSchema) (zng.Type, error) {
	cols := make([]zng.Column, 0, len(schema.Fields))
	for _, fld := range schema.Fields {
		fieldType := fld.Type
		// If this field is a union of one type and the null type,
		// then it is an "optional" field in the avro world.  Since
		// all fields in Zed records can be null (aka optional),
		// we'll just smash this to  its underlying type.  The decoder
		// will decode the values against the avro schema and apply
		// the same object to remove the union-wrapper on each said value.
		if opt := isOptional(fieldType); opt != nil {
			fieldType = opt
		}
		typ, err := DecodeSchema(zctx, fieldType)
		if err != nil {
			return nil, err
		}
		cols = append(cols, zng.Column{fld.Name, typ})
	}
	return zctx.LookupTypeRecord(cols)
}

func isOptional(schema avro.Schema) avro.Schema {
	if union, ok := schema.(*avro.UnionSchema); ok {
		types := union.Types
		if len(types) == 2 {
			if _, ok := types[0].(*avro.NullSchema); ok {
				return types[1]
			}
			if _, ok := types[1].(*avro.NullSchema); ok {
				return types[0]
			}
		}
	}
	return nil
}

func decodeArraySchema(zctx *zson.Context, schema *avro.ArraySchema) (zng.Type, error) {
	inner, err := DecodeSchema(zctx, schema.Items)
	if err != nil {
		return nil, err
	}
	return zctx.LookupTypeArray(inner), nil
}

func decodeUnionSchema(zctx *zson.Context, schema *avro.UnionSchema) (zng.Type, error) {
	types := make([]zng.Type, 0, len(schema.Types))
	for _, avroType := range schema.Types {
		typ, err := DecodeSchema(zctx, avroType)
		if err != nil {
			return nil, err
		}
		types = append(types, typ)
	}
	return zctx.LookupTypeUnion(types), nil
}

func decodeScalarSchema(schema avro.Schema) (zng.Type, error) {
	//XXX IPs need meta-data/alias, could also try to parse string as option
	//XXX meta-data, alias to recover unsigneds?
	switch schema := schema.(type) {
	case *avro.BooleanSchema:
		return zng.TypeBool, nil
	case *avro.LongSchema:
		return zng.TypeInt64, nil
	case *avro.DoubleSchema, *avro.FloatSchema:
		return zng.TypeFloat64, nil
	case *avro.StringSchema:
		return zng.TypeString, nil
	case *avro.NullSchema:
		return zng.TypeNull, nil
	default:
		return nil, fmt.Errorf("unsupported avro schema type: %T", schema)
	}
}
