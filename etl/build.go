package etl

import (
	"errors"
	"fmt"
	"strings"
)

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
	return fmt.Sprintf("split (\n%s)\n", indent(code, 2)), nil
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
	code += "    split (\n"
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
	code += "    split(\n"
	code += fmt.Sprintf("      => kafka.topic==%q | yield {left:this} | sort %s\n", etl.Left, leftKey)
	code += fmt.Sprintf("      => kafka.topic==%q | yield {right:this} | sort %s\n", etl.Right, rightKey)
	code += "    )\n"
	code += fmt.Sprintf("    | join on %s=%s right:=right\n", leftKey, rightKey)
	code += "    | split (\n"
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
