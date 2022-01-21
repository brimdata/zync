package zavro

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/zcode"
)

// These errors shouldn't happen because the input should be type checked.
var ErrBadValue = errors.New("bad zng value in kavro translator")

func Encode(dst []byte, id uint32, zv zed.Value) ([]byte, error) {
	// build kafka/avro header
	var hdr [5]byte
	hdr[0] = 0
	binary.BigEndian.PutUint32(hdr[1:], uint32(id))
	dst = append(dst, hdr[:]...)
	return encodeAny(dst, zv)
}

//XXX move this to zed/zcode.
func zlen(zv zcode.Bytes) (int, error) {
	it := zcode.Iter(zv)
	cnt := 0
	for !it.Done() {
		it.Next()
		cnt++
	}
	return cnt, nil
}

func encodeAny(dst []byte, zv zed.Value) ([]byte, error) {
	switch typ := zed.TypeUnder(zv.Type).(type) {
	case *zed.TypeRecord:
		return encodeRecord(dst, typ, zv.Bytes)
	case *zed.TypeArray:
		return encodeArray(dst, typ.Type, zv.Bytes)
	case *zed.TypeSet:
		// encode set as array
		return encodeArray(dst, typ.Type, zv.Bytes)
	default:
		return encodeScalar(dst, typ, zv.Bytes)
	}
}

func encodeArray(dst []byte, elemType zed.Type, body zcode.Bytes) ([]byte, error) {
	if body == nil {
		return dst, nil
	}
	cnt, err := zlen(body)
	if err != nil {
		return nil, err
	}
	dst = appendVarint(dst, int64(cnt))
	for it := body.Iter(); !it.Done(); {
		dst, err = encodeAny(dst, *zed.NewValue(elemType, it.Next()))
		if err != nil {
			return nil, err
		}
	}
	if cnt != 0 {
		// append 0-length block to indicate end of array
		dst = appendVarint(dst, int64(0))
	}
	return dst, nil
}

func encodeRecord(dst []byte, typ *zed.TypeRecord, body zcode.Bytes) ([]byte, error) {
	if body == nil {
		return dst, nil
	}
	it := body.Iter()
	for _, col := range typ.Columns {
		if it.Done() {
			return nil, ErrBadValue
		}
		body := it.Next()
		if body == nil {
			// unset field.  encode as the null type.
			dst = appendVarint(dst, 0)
			continue
		}
		// field is present.  encode the field union by referencing
		// the type's position in the union.
		dst = appendVarint(dst, 1)
		var err error
		dst, err = encodeAny(dst, *zed.NewValue(col.Type, body))
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func encodeScalar(dst []byte, typ zed.Type, body zcode.Bytes) ([]byte, error) {
	if body == nil {
		//XXX need to encode empty stuff
		return dst, nil
	}
	switch typ.ID() {
	case zed.IDUint8, zed.IDUint16, zed.IDUint32, zed.IDUint64:
		// count is encoded as a uint64.  XXX return error on overdflow?
		return appendVarint(dst, int64(zed.DecodeUint(body))), nil
	case zed.IDInt8, zed.IDInt16, zed.IDInt32, zed.IDInt64:
		return appendVarint(dst, zed.DecodeInt(body)), nil
	case zed.IDDuration, zed.IDTime:
		// Map nanoseconds to microsecond.
		us := zed.DecodeInt(body) / 1000
		return appendVarint(dst, int64(us)), nil
	case zed.IDFloat32:
		if len(body) != 4 {
			return nil, errors.New("float32 value not 4 bytes")
		}
		return append(dst, body...), nil
	case zed.IDFloat64:
		if len(body) != 8 {
			return nil, errors.New("float64 value not 8 bytes")
		}
		return append(dst, body...), nil
	case zed.IDBool:
		// bool is single byte 0 or 1
		if zed.DecodeBool(body) {
			return append(dst, byte(1)), nil
		}
		return append(dst, byte(0)), nil
	case zed.IDBytes, zed.IDString:
		return appendCountedValue(dst, body), nil
	case zed.IDIP:
		b := []byte(zed.DecodeIP(body).String())
		return appendCountedValue(dst, b), nil
	case zed.IDNet:
		b := []byte(zed.DecodeNet(body).String())
		return appendCountedValue(dst, b), nil
	case zed.IDType:
		b := []byte(zed.FormatTypeValue(body))
		return appendCountedValue(dst, b), nil
	case zed.IDNull:
		return dst, nil
	default:
		return nil, fmt.Errorf("encodeScalar: unknown type: %q", typ)
	}
}

func appendVarint(dst []byte, v int64) []byte {
	var encoding [binary.MaxVarintLen64]byte
	n := binary.PutVarint(encoding[:], v)
	return append(dst, encoding[:n]...)
}

func appendCountedValue(dst, val []byte) []byte {
	dst = appendVarint(dst, int64(len(val)))
	return append(dst, val...)
}
