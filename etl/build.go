package etl

import (
	"errors"
	"fmt"
	"strings"
)

// Currently, we take a brute force approach and compile a Zed script for
// each output topic from scratch and we scan the entire range of each pool
// for each step.  Later we can range-limit the scans based on the cursor of
// each topic (note in this more generalized approach the cursor of an input
// topic requires info from possible more than one output topic, but maybe
// we won't handle this more generalized case until later).  For now, we require
// all the input topics go through ETLs that land in the same *pool* but do not
// require that they all land in the same output topic.  This way a cursor
// query on the *pool* can reliably give us the cursor and the completed offsets
// for anti-join.

func Build(transform *Transform) ([]string, error) {
	routes, err := newRoutes(transform)
	if err != nil {
		return nil, err
	}
	// First, for each output topic, we compute all of the input topics
	// needed by all of the ETLs to that output.  Note that an input
	// topic may be routed to multiple output topics but all of those
	// topics must be in the same pool (XXX add a check for this).
	for _, etl := range transform.ETLs {
		switch etl.Type {
		case "denorm":
			if etl.Left == "" || etl.Right == "" {
				return nil, errors.New("both 'left' and 'right' topics must be specified for denorm ETL")
			}
			if etl.In != "" {
				return nil, errors.New("'in' topic cannot be specified for denorm ETL")
			}
			if err := routes.enter(etl.Left, etl.Out); err != nil {
				return nil, err
			}
			if err := routes.enter(etl.Right, etl.Out); err != nil {
				return nil, err
			}
		case "stateless":
			if etl.In == "" {
				return nil, errors.New("'in' topic must be specified for stateless ETL")
			}
			if etl.Left != "" || etl.Right != "" {
				return nil, errors.New("'left' or 'right' topic cannot be specified for stateless ETL")
			}
			if err := routes.enter(etl.In, etl.Out); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unknown ETL type: %q", etl.Type)
		}
	}
	// For every input topic, we now know what output pool it is rounted to.
	// There may be multiple output topics for a given input, but they all
	// land in the same pool.

	// For each output topic, we'll build a Zed for all the ETLs that
	// land on that topic.   It's okay to have multiple ETLs landing
	// on the same topic (e.g., multiple ETLs from different tables
	// land on one denormalized table).

	var zeds []string
	for _, outputTopic := range routes.Outputs() {
		var etls []Rule
		for _, etl := range transform.ETLs {
			if etl.Out == outputTopic {
				etls = append(etls, etl)
			}
		}
		inputTopics := routes.InputsOf(outputTopic)
		s, err := buildZed(inputTopics, outputTopic, routes, etls)
		if err != nil {
			return nil, err
		}
		zeds = append(zeds, s)
	}
	return zeds, nil
}

func buildZed(inputTopics []string, outputTopic string, routes *Routes, etls []Rule) (string, error) {
	code, err := buildFrom(inputTopics, outputTopic, routes)
	if err != nil {
		return "", err
	}
	code = "type done = {kafka:{topic:string,offset:int64}}\n" + code
	code += "| filter *\n" //XXX switch can't handle multiple parents
	code += "| switch (\n"
	var ndefault int
	for _, etl := range etls {
		switch etl.Type {
		case "stateless":
			code += buildStateless(etl)
		case "denorm":
			denorm, err := buildDenorm(etl)
			if err != nil {
				return "", err
			}
			code += denorm
		default:
			return "", fmt.Errorf("unknown ETL type: %q", etl.Type)
		}
	}
	if ndefault > 1 {
		return "", errors.New("only one default (or blank) selector is allowed")
	}
	code += ")\n| sort kafka.offset\n"
	return code, nil
}

const fromTemplate = `
from (
  pool %q => kafka.topic==%q
  pool %q => is(<done>) kafka.topic==%q
) | anti join on kafka.offset=kafka.offset
`

func buildFrom(inputTopics []string, outputTopic string, routes *Routes) (string, error) {
	switch len(inputTopics) {
	case 0:
		//XXX can't happen
		return "", errors.New("no input topics found")
	case 1:
		inTopic := inputTopics[0]
		inPool := routes.LookupPool(inTopic)
		outPool := routes.LookupPool(outputTopic)
		return fmt.Sprintf(fromTemplate, inPool, inTopic, outPool, inTopic), nil
	}
	var code string
	for k := range inputTopics {
		s, err := buildFrom(inputTopics[k:k+1], outputTopic, routes)
		if err != nil {
			return "", err
		}
		code += "=> " + strings.TrimLeft(s, "\n") + "\n"
	}
	return fmt.Sprintf("fork (\n%s)\n", indent(code, 2)), nil
}

func indent(s string, tab int) string {
	lines := strings.Split(s, "\n")
	bump := strings.Repeat(" ", tab)
	out := strings.Join(lines, "\n"+bump)
	return bump + strings.TrimSpace(out) + "\n"
}

func formatZedHead(s string, tab int) string {
	s = strings.TrimSpace(s)
	if s[0] == '|' {
		s = s[1:]
	}
	return "  " + indent(s, tab)
}

func formatZed(s string, tab int) string {
	s = strings.TrimSpace(s)
	if s[0] != '|' {
		s = "| " + s
	}
	return indent(s, tab)
}

func buildStateless(etl Rule) string {
	var where string
	if etl.Where != "" {
		where = fmt.Sprintf("(%s) and ", etl.Where)
	}
	code := fmt.Sprintf("  case %skafka.topic==%q =>\n", where, etl.In)
	code += "    fork (\n"
	code += "      =>\n"
	code += "        yield {in:this}\n"
	code += "\n    // === user-defined ETL ===\n"
	code += formatZed(etl.Zed, 8)
	code += "\n"
	//XXX should embed error if user doesn't create the output field
	code += "        | out.kafka:=in.kafka\n"
	code += "        | yield out\n"
	code += fmt.Sprintf("        | kafka.topic:=%q\n", etl.Out)
	code += "        \n"
	code += "      =>\n"
	code += "        yield cast({kafka:{topic:kafka.topic,offset:kafka.offset}},done)\n"
	code += "        \n"
	code += "      )\n"
	return code
}

func buildDenorm(etl Rule) (string, error) {
	keys := strings.Split(etl.Join, "=")
	if len(keys) != 2 {
		if etl.Join == "" {
			return "", errors.New("no join-on expression provided in denorm rule")
		}
		return "", fmt.Errorf("join-on syntax error: %q", etl.Join)
	}
	leftKey := strings.TrimSpace(keys[0])
	rightKey := strings.TrimSpace(keys[1])
	code := fmt.Sprintf("  case %s =>\n", etl.Where)
	code += "    fork (\n"
	code += fmt.Sprintf("      => kafka.topic==%q | yield {left:this} | sort %s\n", etl.Left, leftKey)
	code += fmt.Sprintf("      => kafka.topic==%q | yield {right:this} | sort %s\n", etl.Right, rightKey)
	code += "    )\n"
	code += fmt.Sprintf("    | join on %s=%s right:=right\n", leftKey, rightKey)
	code += "    | fork (\n"
	code += "      =>\n"
	code += "          // === user-defined ETL ===\n"
	code += formatZedHead(etl.Zed, 8)
	code += "        | out.kafka:=left.kafka\n"
	code += "        | yield out\n"
	code += fmt.Sprintf("        | kafka.topic:=%q\n", etl.Out)
	code += "      =>  yield {\n"
	code += "             left:cast({kafka:{topic:left.kafka.topic,offset:left.kafka.offset}},done),\n"
	code += "             right:cast({kafka:{topic:right.kafka.topic,offset:right.kafka.offset}},done)\n"
	code += "          }\n"
	code += "    )\n"
	return code, nil
}
