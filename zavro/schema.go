package zavro

import (
	"crypto/md5"
	"fmt"
	"reflect"

	"github.com/go-avro/avro"
	"github.com/mccanne/zq/pkg/zeek"
)

func GenSchema(typ zeek.Type) avro.Schema {
	switch typ := typ.(type) {
	case *zeek.TypeRecord:
		return genRecordSchema(typ)
	case *zeek.TypeVector:
		return genVectorSchema(typ)
	case *zeek.TypeSet:
		return genSetSchema(typ)
	default:
		return genScalarSchema(typ)
	}
}

func genVectorSchema(typ *zeek.TypeVector) avro.Schema {
	inner := zeek.InnerType(typ)
	return &avro.ArraySchema{
		Items: GenSchema(inner),
	}
}

func genSetSchema(typ *zeek.TypeSet) avro.Schema {
	// XXX this looks the same as vector for now but we will want to add
	// more meta-info to disnguish the two cases
	inner := zeek.InnerType(typ)
	return &avro.ArraySchema{
		Items: GenSchema(inner),
	}
}

func genRecordSchema(typ *zeek.TypeRecord) avro.Schema {
	var fields []*avro.SchemaField
	for _, col := range typ.Columns {
		fld := &avro.SchemaField{
			Name: col.Name,
			Type: GenSchema(col.Type),
		}
		fields = append(fields, fld)
	}
	namespace := "com.example" //XXX
	// We hash the zng type to an md5 fingerprint here, otherwise
	// we would get a ton of versions on the same name for different
	// instances/restarts of a zng stream.
	sum := md5.Sum([]byte(typ.String()))
	return &avro.RecordSchema{
		Name:       fmt.Sprintf("zng_%x", sum),
		Namespace:  namespace,
		Doc:        "",
		Aliases:    nil,
		Properties: nil,
		Fields:     fields,
	}
}

func genScalarSchema(typ zeek.Type) avro.Schema {
	switch typ.(type) {
	case *zeek.TypeOfAddr:
		// IP addresses are turned into strings...
		return &avro.StringSchema{}

	case *zeek.TypeOfBool:
		return &avro.BooleanSchema{}

	case *zeek.TypeOfCount:
		return &avro.LongSchema{}

	case *zeek.TypeOfDouble:
		return &avro.DoubleSchema{}

	case *zeek.TypeOfEnum:
		// for now, we change zng enums to avro strings.
		// we would like to change enum to a conventional enum
		// but zeek doesn't provide the enum def so we just
		// cast zeek enum values to string values
		return &avro.StringSchema{}

	case *zeek.TypeOfInt:
		// zng int is an avro long
		return &avro.LongSchema{}

	case *zeek.TypeOfInterval:
		return &MicroTimeSchema{}

	case *zeek.TypeOfPort:
		// XXX map a port to an int
		return &avro.IntSchema{}

	case *zeek.TypeOfString:
		return &avro.StringSchema{}

	case *zeek.TypeOfSubnet:
		return &avro.StringSchema{}

	case *zeek.TypeOfTime:
		return &MicroTimeSchema{}

	default:
		panic("genScalarSchema: unknown type")
	}
}

// MicroTimeSchema implements avro.Schema and represents Avro long type.
type MicroTimeSchema struct{}

// Returns a JSON representation of LongSchema.
func (*MicroTimeSchema) String() string {
	return `{"type": "time-micros", "name": "time-micros", "logicalType": "long" }`
}

// Type returns a type constant for this MicroTimeSchema.
func (*MicroTimeSchema) Type() int {
	return avro.Long
}

// GetName returns a type name for this MicroTimeSchema.
func (*MicroTimeSchema) GetName() string {
	return "long"
}

// Prop doesn't return anything valuable for LongSchema.
func (*MicroTimeSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (*MicroTimeSchema) Validate(v reflect.Value) bool {
	//return reflect.TypeOf(dereference(v).Interface()).Kind() == reflect.Int64
	//XXX
	return true
}

// MarshalJSON serializes the given schema as JSON. Never returns an error.
func (*MicroTimeSchema) MarshalJSON() ([]byte, error) {
	return []byte(`{ "type" : "long", "logicalType" : "time-micros" }`), nil
}
