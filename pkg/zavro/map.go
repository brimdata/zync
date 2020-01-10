package zavro

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/go-avro/avro"
)

type FieldMap struct {
	Input  string `json:"input"`
	Output string `json:"output"`
}

type Rule struct {
	Path   string     `json:"path"`
	Avro   string     `json:"avro"`
	Fields []FieldMap `json:"fields,omitempty"`
}

func LoadConfig(schemaDir, configFile string) (map[string]*Route, error) {
	rules, err := LoadRules(configFile)
	if err != nil {
		return nil, err
	}
	schemas, err := LoadSchemas(schemaDir)
	if err != nil {
		return nil, err
	}
	return BuildRouter(rules, schemas)
}

func LoadRules(filename string) ([]Rule, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var rules []Rule
	err = json.Unmarshal(data, &rules)
	return rules, err
}

func LoadSchemas(dir string) (map[string]avro.Schema, error) {
	table := avro.LoadSchemas(dir)
	if len(table) == 0 {
		return nil, fmt.Errorf("unable to load schemas from: %s", dir)
	}
	return table, nil
}

type Route struct {
	Schema     *avro.RecordSchema
	InputField []string
}

func lookup(fields []*avro.SchemaField, name string) *avro.SchemaField {
	for _, f := range fields {
		if f.Name == name {
			return f
		}
	}
	return nil
}

func BuildRouter(rules []Rule, table map[string]avro.Schema) (map[string]*Route, error) {
	out := make(map[string]*Route)
	for _, r := range rules {
		schema, ok := table[r.Avro]
		if !ok {
			return nil, fmt.Errorf("%s: schema not found in rule for %s", r.Avro, r.Path)
		}
		recSchema, ok := schema.(*avro.RecordSchema)
		if !ok {
			return nil, fmt.Errorf("%s: schema not a record type", r.Avro)
		}
		route := &Route{
			Schema: recSchema,
		}
		for _, field := range r.Fields {
			outputField := lookup(recSchema.Fields, field.Output)
			if outputField == nil {
				return nil, fmt.Errorf("field %s not found in schema %s", field.Output, r.Avro)
			}
			route.InputField = append(route.InputField, field.Input)
		}
		out[r.Path] = route
	}
	return out, nil
}
