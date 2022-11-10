// Package connectjson implements encoding of Zed values to the format produced
// by org.apache.kafka.connect.json.JsonConverter and decoding of Zed values
// from that format.
package connectjson

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/runtime/expr"
	"github.com/brimdata/zed/zcode"
	"github.com/brimdata/zed/zio/jsonio"
	"github.com/brimdata/zed/zson"
	"github.com/buger/jsonparser"
)

type connectSchema struct {
	Type     string           `json:"type"`
	Fields   []*connectSchema `json:"fields,omitempty"`
	Optional bool             `json:"optional"`
	Name     string           `json:"name,omitempty"`
	Field    string           `json:"field,omitempty"`
}

func Encode(val *zed.Value) ([]byte, error) {
	if zed.TypeUnder(val.Type) == zed.TypeNull {
		return nil, nil
	}
	schema, err := marshalSchema(val.Type)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Schema  *connectSchema `json:"schema"`
		Payload interface{}    `json:"payload"`
	}{
		schema, marshalPayload(val.Type, val.Bytes),
	})
}

func marshalPayload(typ zed.Type, bytes zcode.Bytes) interface{} {
	if bytes == nil {
		return nil
	}
	switch typ := zed.TypeUnder(typ).(type) {
	case *zed.TypeOfUint8, *zed.TypeOfUint16, *zed.TypeOfUint32, *zed.TypeOfUint64:
		return zed.DecodeUint(bytes)
	case *zed.TypeOfInt8, *zed.TypeOfInt16, *zed.TypeOfInt32, *zed.TypeOfInt64,
		*zed.TypeOfDuration, *zed.TypeOfTime:
		return zed.DecodeInt(bytes)
	case *zed.TypeOfFloat32, *zed.TypeOfFloat64:
		return zed.DecodeFloat(bytes)
	case *zed.TypeOfBool:
		return zed.DecodeBool(bytes)
	case *zed.TypeOfBytes:
		return base64.StdEncoding.EncodeToString(bytes)
	case *zed.TypeOfString:
		return string(bytes)
	case *zed.TypeOfIP:
		return zed.DecodeIP(bytes).String()
	case *zed.TypeOfNet:
		return zed.DecodeNet(bytes).String()
	case *zed.TypeOfNull:
		return nil
	case *zed.TypeRecord:
		m := map[string]interface{}{}
		it := bytes.Iter()
		for _, col := range typ.Columns {
			m[col.Name] = marshalPayload(col.Type, it.Next())
		}
		return m
	case *zed.TypeArray, *zed.TypeSet:
		a := []interface{}{}
		inner := zed.InnerType(typ)
		for it := bytes.Iter(); !it.Done(); {
			a = append(a, marshalPayload(inner, it.Next()))
		}
		return a
	case *zed.TypeMap:
		panic("map type unsupported")
	case *zed.TypeUnion:
		panic("union type unsupported")
	case *zed.TypeEnum:
		// Trim leading "%".
		return zson.MustFormatValue(zed.NewValue(typ, bytes))[1:]
	case *zed.TypeError:
		return zson.MustFormatValue(zed.NewValue(typ, bytes))
	default:
		panic(fmt.Sprintf("%T unsupported", typ))
	}
}

func marshalSchema(typ zed.Type) (*connectSchema, error) {
	schema := &connectSchema{Optional: true}
	if named, ok := typ.(*zed.TypeNamed); ok {
		schema.Name = named.Name
	}
	switch typ.Kind() {
	case zed.PrimitiveKind:
		switch typ.ID() {
		case zed.IDUint8:
			schema.Type = "int8"
		case zed.IDUint16:
			schema.Type = "int16"
		case zed.IDUint32:
			schema.Type = "int32"
		case zed.IDUint64:
			schema.Type = "int64"
		case zed.IDInt8:
			schema.Type = "int8"
		case zed.IDInt16:
			schema.Type = "int16"
		case zed.IDInt32:
			schema.Type = "int32"
		case zed.IDInt64, zed.IDDuration, zed.IDTime:
			schema.Type = "int64"
		case zed.IDFloat32:
			schema.Type = "float"
		case zed.IDFloat64:
			schema.Type = "double"
		case zed.IDBool:
			schema.Type = "boolean"
		case zed.IDBytes:
			schema.Type = "bytes"
		case zed.IDString, zed.IDIP, zed.IDNet, zed.IDType:
			schema.Type = "string"
		case zed.IDNull:
			return nil, errors.New("Zed null type unsupported by Connect")
		default:
			panic(fmt.Sprintf("unknown Zed ID %d", typ.ID()))
		}
	case zed.RecordKind:
		schema.Type = "struct"
		for _, c := range zed.TypeRecordOf(typ).Columns {
			f, err := marshalSchema(c.Type)
			if err != nil {
				return nil, err
			}
			f.Field = c.Name
			schema.Fields = append(schema.Fields, f)
		}
	case zed.ArrayKind:
		panic("array type unsupported")
	case zed.SetKind:
		panic("set type unsupported")
	case zed.MapKind:
		panic("map type unsupported")
	case zed.UnionKind:
		panic("union type unsupported")
	case zed.EnumKind, zed.ErrorKind:
		schema.Type = "string"
	default:
		panic(fmt.Sprintf("unknown Zed kind %q", typ.Kind()))
	}
	return schema, nil
}

type Decoder struct {
	zctx *zed.Context

	buf          *bytes.Buffer
	builder      zcode.Builder
	ectx         expr.Context
	jsonioReader *jsonio.Reader
	shapers      map[string]*expr.ConstShaper
	this         expr.This
	val          zed.Value
}

func NewDecoder(zctx *zed.Context) *Decoder {
	buf := &bytes.Buffer{}
	return &Decoder{
		zctx: zctx,

		buf:          buf,
		ectx:         expr.NewContext(),
		jsonioReader: jsonio.NewReader(zctx, buf),
		shapers:      map[string]*expr.ConstShaper{},
	}
}

func (c *Decoder) Decode(b []byte) (*zed.Value, error) {
	b = bytes.TrimSpace(b)
	if len(b) == 0 {
		return zed.Null, nil
	}
	var schema, payload []byte
	err := jsonparser.ObjectEach(b, func(key []byte, value []byte, vt jsonparser.ValueType, _ int) error {
		if vt == jsonparser.String {
			value = []byte(`"` + string(value) + `"`)
		}
		if string(key) == "schema" {
			schema = value
		} else if string(key) == "payload" {
			payload = value
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	c.buf.Reset()
	c.buf.Write(payload)
	val, err := c.jsonioReader.Read()
	if err != nil {
		return nil, err
	}
	// Using the schema's JSON encoding as the key here means different
	// encodings of the same schema won't share an entry, but that should be
	// rare.
	shaper, ok := c.shapers[string(schema)]
	if !ok {
		var cs connectSchema
		if err := json.Unmarshal(schema, &cs); err != nil {
			return nil, err
		}
		_, typ, err := c.decodeSchema(&cs)
		if err != nil {
			return nil, err
		}
		shaper = expr.NewConstShaper(c.zctx, &c.this, typ, expr.Cast|expr.Order)
		c.shapers[string(schema)] = shaper
	}
	return c.decodeBytes(shaper.Eval(c.ectx, val)), nil
}

func (c *Decoder) decodeSchema(s *connectSchema) (string, zed.Type, error) {
	if strings.HasPrefix(s.Name, "org.apache") {
		panic("XXX " + s.Name)
	}
	var typ zed.Type
	var err error
	switch s.Type {
	case "boolean":
		typ = zed.TypeBool
	case "int8":
		typ = zed.TypeInt8
	case "int16":
		typ = zed.TypeInt16
	case "int32":
		typ = zed.TypeInt32
	case "int64":
		typ = zed.TypeInt64
	case "float":
		typ = zed.TypeFloat32
	case "double":
		typ = zed.TypeFloat64
	case "bytes":
		typ = zed.TypeBytes
	case "string":
		typ = zed.TypeString
	case "array":
		err = errors.New("array type unimplemented")
	case "map":
		err = errors.New("map type unimplemented")
	case "struct":
		var cols []zed.Column
		for _, schema := range s.Fields {
			fname, ftype, err := c.decodeSchema(schema)
			if err != nil {
				return "", nil, err
			}
			cols = append(cols, zed.NewColumn(fname, ftype))
		}
		typ, err = c.zctx.LookupTypeRecord(cols)
	default:
		err = fmt.Errorf("unknown type %q in Connect schema", s.Type)
	}
	if err == nil && s.Name != "" {
		typ, err = c.zctx.LookupTypeNamed(s.Name, typ)
	}
	return s.Field, typ, err
}

func (c *Decoder) decodeBytes(val *zed.Value) *zed.Value {
	if val.IsNull() {
		return val
	}
	c.builder.Truncate()
	err := Walk(val.Type, val.Bytes, func(typ zed.Type, bytes zcode.Bytes) error {
		if bytes == nil {
			c.builder.Append(nil)
		} else if zed.IsContainerType(typ) {
			c.builder.BeginContainer()
		} else if typ == nil {
			c.builder.EndContainer()
		} else {
			if typ.ID() == zed.IDBytes && bytes != nil {
				var err error
				bytes, err = base64.StdEncoding.DecodeString(string(bytes))
				if err != nil {
					return err
				}
			}
			c.builder.Append(bytes)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	c.val = *zed.NewValue(val.Type, c.builder.Bytes().Body())
	return &c.val
}

func Walk(typ zed.Type, body zcode.Bytes, visit zed.Visitor) error {
	if err := visit(typ, body); err != nil {
		if err == zed.SkipContainer {
			return nil
		}
		return err
	}
	if body == nil {
		return nil
	}
	switch typ := zed.TypeUnder(typ).(type) {
	case *zed.TypeRecord:
		defer visit(nil, body)
		return walkRecord(typ, body, visit)
	case *zed.TypeArray:
		defer visit(nil, body)
		return walkArray(typ, body, visit)
	case *zed.TypeSet:
		defer visit(nil, body)
		return walkSet(typ, body, visit)
	case *zed.TypeUnion:
		defer visit(nil, body)
		return walkUnion(typ, body, visit)
	case *zed.TypeMap:
		defer visit(nil, body)
		return walkMap(typ, body, visit)
	case *zed.TypeError:
		defer visit(nil, body)
		return Walk(typ.Type, body, visit)
	}
	return nil
}

func walkRecord(typ *zed.TypeRecord, body zcode.Bytes, visit zed.Visitor) error {
	if body == nil {
		return nil
	}
	it := body.Iter()
	for _, col := range typ.Columns {
		if it.Done() {
			return zed.ErrMissingField
		}
		if err := Walk(col.Type, it.Next(), visit); err != nil {
			return err
		}
	}
	return nil
}

func walkArray(typ *zed.TypeArray, body zcode.Bytes, visit zed.Visitor) error {
	if body == nil {
		return nil
	}
	inner := zed.InnerType(typ)
	it := body.Iter()
	for !it.Done() {
		if err := Walk(inner, it.Next(), visit); err != nil {
			return err
		}
	}
	return nil
}

func walkUnion(typ *zed.TypeUnion, body zcode.Bytes, visit zed.Visitor) error {
	if body == nil {
		return nil
	}
	if len(body) == 0 {
		return errors.New("union has empty body")
	}
	it := body.Iter()
	selector := zed.DecodeInt(it.Next())
	inner, err := typ.Type(int(selector))
	if err != nil {
		return err
	}
	body = it.Next()
	if !it.Done() {
		return errors.New("union value container has more than two items")
	}
	return Walk(inner, body, visit)
}

func walkSet(typ *zed.TypeSet, body zcode.Bytes, visit zed.Visitor) error {
	if body == nil {
		return nil
	}
	inner := zed.TypeUnder(zed.InnerType(typ))
	it := body.Iter()
	for !it.Done() {
		if err := Walk(inner, it.Next(), visit); err != nil {
			return err
		}
	}
	return nil
}

func walkMap(typ *zed.TypeMap, body zcode.Bytes, visit zed.Visitor) error {
	if body == nil {
		return nil
	}
	keyType := zed.TypeUnder(typ.KeyType)
	valType := zed.TypeUnder(typ.ValType)
	it := body.Iter()
	for !it.Done() {
		if err := Walk(keyType, it.Next(), visit); err != nil {
			return err
		}
		if err := Walk(valType, it.Next(), visit); err != nil {
			return err
		}
	}
	return nil
}
