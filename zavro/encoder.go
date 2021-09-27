package zavro

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/brimdata/zed/zcode"
	"github.com/brimdata/zed/zng"
)

// These errors shouldn't happen because the input should be type checked.
var ErrBadValue = errors.New("bad zng value in kavro translator")

func Encode(dst []byte, id uint32, zv zng.Value) ([]byte, error) {
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
		_, _, err := it.Next()
		if err != nil {
			return 0, err
		}
		cnt++
	}
	return cnt, nil
}

func encodeAny(dst []byte, zv zng.Value) ([]byte, error) {
	switch typ := zv.Type.(type) {
	case *zng.TypeRecord:
		return encodeRecord(dst, typ, zv.Bytes)
	case *zng.TypeArray:
		return encodeArray(dst, typ.Type, zv.Bytes)
	case *zng.TypeSet:
		// encode set as array
		return encodeArray(dst, typ.Type, zv.Bytes)
	default:
		return encodeScalar(dst, typ, zv.Bytes)
	}
}

func encodeArray(dst []byte, elemType zng.Type, body zcode.Bytes) ([]byte, error) {
	if body == nil {
		return dst, nil
	}
	cnt, err := zlen(body)
	if err != nil {
		return nil, err
	}
	dst = appendVarint(dst, int64(cnt))
	it := zcode.Iter(body)
	for !it.Done() {
		body, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		dst, err = encodeAny(dst, zng.Value{elemType, body})
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

func encodeRecord(dst []byte, typ *zng.TypeRecord, body zcode.Bytes) ([]byte, error) {
	if body == nil {
		return dst, nil
	}
	it := zcode.Iter(body)
	for _, col := range typ.Columns {
		if it.Done() {
			return nil, ErrBadValue
		}
		body, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		if body == nil {
			// unset field.  encode as the null type.
			dst = appendVarint(dst, 0)
			continue
		}
		// field is present.  encode the field union by referencing
		// the type's position in the union.
		dst = appendVarint(dst, 1)
		dst, err = encodeAny(dst, zng.Value{col.Type, body})
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func encodeScalar(dst []byte, typ zng.Type, body zcode.Bytes) ([]byte, error) {
	if body == nil {
		//XXX need to encode empty stuff
		return dst, nil
	}
	switch typ.ID() {
	case zng.IDNull:
		return dst, nil
	case zng.IDIP:
		// IP addresses are turned into strings...
		ip, err := zng.DecodeIP(body)
		if err != nil {
			return nil, err
		}
		b := []byte(ip.String())
		return appendCountedValue(dst, b), nil
	case zng.IDBool:
		// bool is single byte 0 or 1
		v, err := zng.DecodeBool(body)
		if err != nil {
			return nil, err
		}
		if v {
			return append(dst, byte(1)), nil
		}
		return append(dst, byte(0)), nil
	case zng.IDInt64:
		v, err := zng.DecodeInt(body)
		if err != nil {
			return nil, err
		}
		return appendVarint(dst, v), nil
	case zng.IDUint64:
		// count is encoded as a uint64.  XXX return error on overdflow?
		v, err := zng.DecodeUint(body)
		if err != nil {
			return nil, err
		}
		return appendVarint(dst, int64(v)), nil
	case zng.IDFloat64:
		// avro says this is Java's doubleToLongBits...
		// we need to check if Go math lib is the same
		if len(body) != 8 {
			return nil, errors.New("double value not 8 bytes")
		}
		return append(dst, body...), nil
	case zng.IDDuration:
		// XXX map an interval to a microsecond time
		ns, err := zng.DecodeDuration(body)
		if err != nil {
			return nil, err
		}
		us := ns / 1000
		return appendVarint(dst, int64(us)), nil
	case zng.IDString, zng.IDBstring:
		return appendCountedValue(dst, []byte(body)), nil
	case zng.IDNet:
		net, err := zng.DecodeNet(body)
		if err != nil {
			return nil, err
		}
		b := []byte(net.String())
		return appendCountedValue(dst, b), nil
	case zng.IDTime:
		// map a nano to a microsecond time
		ts, err := zng.DecodeInt(body)
		if err != nil {
			return nil, err
		}
		us := ts / 1000
		return appendVarint(dst, us), nil
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
