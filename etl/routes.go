package etl

import (
	"fmt"
)

type Routes struct {
	pools   map[string]string   // any topic to pool
	inputs  map[string][]string // output topics of the input
	outputs map[string][]string // input topics of the output
}

func newRoutes(transform *Transform) (*Routes, error) {
	pools := make(map[string]string)
	all := make([]Route, 0, len(transform.Inputs)+1)
	all = append(append(all, transform.Inputs...), transform.Output)
	for _, route := range all {
		if _, ok := pools[route.Topic]; ok {
			return nil, fmt.Errorf("route for topic %q points to multiple pools", route.Topic)
		}
		pools[route.Topic] = route.Pool
	}
	return &Routes{
		pools:   pools,
		inputs:  make(map[string][]string),
		outputs: make(map[string][]string),
	}, nil
}

func (r *Routes) LookupPool(topic string) string {
	return r.pools[topic]
}

func (r *Routes) Outputs() []string {
	topics := make([]string, 0, len(r.outputs))
	for topic := range r.outputs {
		topics = append(topics, topic)
	}
	return topics
}

func (r *Routes) InputsOf(output string) []string {
	return r.outputs[output]
}

func (r *Routes) checkpool(topic string) error {
	if _, ok := r.pools[topic]; !ok {
		return fmt.Errorf("topic %q has unknown pool", topic)
	}
	return nil
}

func (r *Routes) enter(input, output string) error {
	if err := r.checkpool(input); err != nil {
		return err
	}
	if err := r.checkpool(output); err != nil {
		return err
	}
	if !stringIn(input, r.outputs[output]) {
		r.outputs[output] = append(r.outputs[output], input)
	}
	inputs := r.inputs
	if !stringIn(output, inputs[input]) {
		// Enforce constraint that all input topics must land on the same pool.
		outputs := inputs[input]
		if len(outputs) > 0 && r.pools[outputs[0]] != r.pools[output] {
			return fmt.Errorf("input topic %q routed to multiple pools (%q and %q)", input, r.pools[outputs[0]], r.pools[output])
		}
		r.inputs[input] = append(r.inputs[input], output)
	}
	return nil
}

func stringIn(s string, list []string) bool {
	for _, elem := range list {
		if s == elem {
			return true
		}
	}
	return false
}
