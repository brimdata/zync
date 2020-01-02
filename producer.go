package zinger

import (
	"github.com/mccanne/zq/pkg/zng"
)

//XXX TBD
type Producer struct {
	Registry *Registry
}

//XXX could have config to map records onto different topics based on path
func (p *Producer) Write(rec *zng.Record) error {
	return nil
}
