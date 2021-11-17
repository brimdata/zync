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
	code = "type done = {kafka:{topic:string,offset:int64}};\n" + code
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
  %q => kafka.topic==%q;
  %q => is(type(done)) kafka.topic==%q;
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
		code += "=> " + strings.TrimLeft(s, "\n") + ";\n"
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
	code := fmt.Sprintf("  %s kafka.topic==%q =>\n", etl.Where, etl.In)
	code += "    split (\n"
	code += "      =>\n"
	code += fmt.Sprintf("        this[%q]:=value.after\n", etl.In)
	code += "\n    // === user-defined ETL ===\n"
	code += formatZed(etl.Zed, 8)
	code += "\n"
	//XXX should embed error if user doesn't create the output field
	code += fmt.Sprintf("        | kafka.topic:=%q,value.after:=this[%q]\n", etl.Out, etl.Out)
	code += fmt.Sprintf("        | drop this[%q]\n", etl.Out)
	code += "        ;\n"
	code += "      =>\n"
	code += "        this:=cast({kafka:{topic:kafka.topic,offset:kafka.offset}},done)\n"
	code += "        ;\n"
	code += "      )\n"
	code += "    ;\n"
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
	code := fmt.Sprintf("  %s =>\n", etl.Where)
	code += "    split(\n"
	rightRawKey := strings.Replace(rightKey, etl.Right, "value.after", 1)
	code += fmt.Sprintf("      => kafka.topic==%q | this[%q]:=value.after,left:=kafka | sort %s;\n", etl.Left, etl.Left, leftKey)
	code += fmt.Sprintf("      => kafka.topic==%q | sort %s;\n", etl.Right, rightRawKey)
	code += "    )\n"
	code += fmt.Sprintf("    | join on %s=%s %s:=value.after,right:=kafka\n", leftKey, rightRawKey, etl.Right)
	code += "    | split (\n"
	code += "      =>\n"
	code += "          // === user-defined ETL ===\n"
	code += formatZedHead(etl.Zed, 8)
	code += fmt.Sprintf("\n        | kafka.topic:=%q,value.after:=this[%q]\n", etl.Out, etl.Out)
	code += fmt.Sprintf("        | drop this[%q],this[%q],this[%q],left,right\n", etl.Left, etl.Right, etl.Out)
	code += "        ;\n"
	code += "      =>  this:={\n"
	code += "             left:cast({kafka:{topic:left.topic,offset:left.offset}},done),\n"
	code += "             right:cast({kafka:{topic:right.topic,offset:right.offset}},done)\n"
	code += "          }\n"
	code += "        ;\n"
	code += "    )\n"
	code += "    ;\n"
	return code, nil
}
