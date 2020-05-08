package zavro

import (
	"reflect"

	"github.com/go-avro/avro"
)

// MicroTimeSchema implements avro.Schema and represents Avro long type.
type MicroTimeSchema struct{}

var _ avro.Schema = (*MicroTimeSchema)(nil)

// Returns a JSON representation of LongSchema.
func (*MicroTimeSchema) String() string {
	return `{"type": "long", "name": "timestamp-micros", "logicalType": "timestamp-micros" }`
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
	return []byte(`{ "type" : "long", "logicalType" : "timestamp-micros" }`), nil
}
