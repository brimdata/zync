package etl

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Transform struct {
	Inputs []Route `yaml:"inputs"`
	Output Route   `yaml:"output"`
	ETLs   []Rule  `yaml:"transforms"`
}

type Route struct {
	Topic string `yaml:"topic"`
	Pool  string `yaml:"pool"`
}

type Rule struct {
	Type  string `yaml:"type"`
	In    string `yaml:"in"`
	Left  string `yaml:"left"`
	Right string `yaml:"right"`
	Join  string `yaml:"join-on"`
	Out   string `yaml:"out"`
	Where string `yaml:"where"`
	Zed   string `yaml:"zed"`
}

func (t *Transform) Load(path string) error {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(b, t)
}

func Load(path string) (*Transform, error) {
	var t Transform
	if err := t.Load(path); err != nil {
		return nil, err
	}
	return &t, nil
}
