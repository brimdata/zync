package connectjson

import (
	"testing"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/zson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectJSON(t *testing.T) {
	var cases = []string{
		`null`,
		`true`,
		`8(int8)`,
		`16(int16)`,
		`32(int32)`,
		`64`,
		`32.(float32)`,
		`64.`,
		`0x0123456789`,
		`"abcd"`,
		`null({})`,
		`null(named={})`,
		`{}`,
		`{}(=named)`,
		`{a:1}`,
		`{a:null({})}`,
		`{a:null(named={})}`,
		`{key:{id:10(int32)}}`,
		`{key:{id:10(int32)},value:{before:{id:1(int32),customer_id:2(int32),street:"street",city:"city",state:"state",zip:"zip",type:"type"}}}`,
		`{key:{id:10(int32)},value:{before:null({id:int32,customer_id:int32,street:string,city:string,state:string,zip:string,type:string})}}`,
		`{key:{id:10(int32)},value:{before:null({id:int32,customer_id:int32,street:string,city:string,state:string,zip:string,type:string}),after:{id:10(int32),customer_id:1001(int32),street:"3183 Moore Avenue",city:"Euless",state:"Texas",zip:"76036",type:"SHIPPING"},source:{version:"1.7.2.Final",connector:"mysql",name:"mysqlserver1",ts_ms:1644503374812,snapshot:"true",db:"inventory",sequence:null(string),table:"addresses",server_id:0,gtid:null(string),file:"mysql-bin.000003",pos:157,row:0(int32),thread:null(int64),query:null(string)},op:"r",ts_ms:1644503374813,transaction:null({id:string,total_order:int64,data_collection_order:int64})}}`,
	}
	for _, s := range cases {
		zctx := zed.NewContext()
		expected, err := zson.ParseValue(zctx, s)
		require.NoError(t, err, s)
		b, err := Encode(expected)
		require.NoError(t, err, s)
		actual, err := NewDecoder(zctx).Decode(b)
		require.NoError(t, err)
		assert.Equal(t, expected, actual, s)
	}
}

func TestConnectJSONDecodeEmptyEnvelope(t *testing.T) {
	for _, b := range [][]byte{nil, {}, []byte(" \n")} {
		val, err := NewDecoder(zed.NewContext()).Decode(b)
		require.NoError(t, err, b)
		assert.Equal(t, zed.Null, val, b)
	}
}
