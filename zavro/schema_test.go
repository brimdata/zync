package zavro

import (
	"testing"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/zson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeSchemaNullTypeRecordFielBecomeNullNotUnion(t *testing.T) {
	typ, err := zson.ParseType(zed.NewContext(), "{a:null}")
	require.NoError(t, err)
	schema, err := EncodeSchema(typ, "namespace")
	require.NoError(t, err)
	const expected = `
{
    "type": "record",
    "namespace": "namespace",
    "name": "zng_4f5c13d8a692b16d2a7d297f951880a3",
    "doc": "Created by zync from zng type {a:null}",
    "fields": [
        {
            "name": "a",
            "default": null,
            "type": "null"
        }
    ]
}`
	assert.JSONEq(t, expected, schema.String(), schema.String())
}

func TestEncodeSchemaRepeatedRecordBecomesReference(t *testing.T) {
	typ, err := zson.ParseType(zed.NewContext(), "{a:{},b:{}}")
	require.NoError(t, err)
	schema, err := EncodeSchema(typ, "namespace")
	require.NoError(t, err)
	const expected = `
{
    "type": "record",
    "namespace": "namespace",
    "name": "zng_2d7e63a29282715120ae93531a98c9ef",
    "doc": "Created by zync from zng type {a:{},b:{}}",
    "fields": [
        {
            "name": "a",
            "default": null,
            "type": [
                "null",
                {
                    "type": "record",
                    "namespace": "namespace",
                    "name": "zng_99914b932bd37a50b983c5e7c90ae93b",
                    "doc": "Created by zync from zng type {}",
                    "fields": null
                }
            ]
        },
        {
            "name": "b",
            "default": null,
            "type": [
                "null",
                "zng_99914b932bd37a50b983c5e7c90ae93b"
            ]
        }
    ]
}`
	assert.JSONEq(t, expected, schema.String())
}
