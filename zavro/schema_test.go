package zavro

import (
	"testing"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/zson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeSchemaJSONFieldOrderMatchesJava(t *testing.T) {
	typ, err := zson.ParseType(zed.NewContext(), "{a:{b:{}}}")
	require.NoError(t, err)
	schema, err := EncodeSchema(typ, "namespace")
	require.NoError(t, err)
	const expected = `{
    "type": "record",
    "name": "zng_228c5f7a92fa77715f4dabe46739bfa3",
    "namespace": "namespace",
    "doc": "Created by zync from zng type {a:{b:{}}}",
    "fields": [
        {
            "name": "a",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "zng_eeb636be88d6a4d3387b3820995db8e7",
                    "namespace": "namespace",
                    "doc": "Created by zync from zng type {b:{}}",
                    "fields": [
                        {
                            "name": "b",
                            "type": [
                                "null",
                                {
                                    "type": "record",
                                    "name": "zng_99914b932bd37a50b983c5e7c90ae93b",
                                    "namespace": "namespace",
                                    "doc": "Created by zync from zng type {}",
                                    "fields": null
                                }
                            ],
                            "default": null
                        }
                    ]
                }
            ],
            "default": null
        }
    ]
}`
	// Equal instead of JSONEq because JSON field order matters for this test.
	assert.Equal(t, expected, schema.String(), schema.String())
}

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
