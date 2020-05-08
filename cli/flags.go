package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Flags struct {
	Topic     string
	Namespace string
}

type APIKey struct {
	User     string
	Password string
}

func (f *Flags) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&f.Topic, "t", "", "Kafka topic name")
	fs.StringVar(&f.Namespace, "n", "io.brimdata.zinger", "Kafka name space for new schemas")
}

func SchemaRegistryEndpoint() (string, APIKey, error) {
	key, err := getKey()
	if err != nil {
		return "", APIKey{}, err
	}
	return key.URL, APIKey{key.User, key.Password}, nil
}

type apiKey struct {
	URL      string `json:"url"`
	User     string `json:"user"`
	Password string `json:"password"`
}

func getKey() (apiKey, error) {
	//XXX move this to CLI flags
	home := os.Getenv("HOME")
	if home == "" {
		return apiKey{}, errors.New("No HOME")
	}
	path := filepath.Join(home, ".confluent", "schema_registry.json")
	b, err := os.ReadFile(path)
	if err != nil {
		return apiKey{}, err
	}
	var key apiKey
	err = json.Unmarshal(b, &key)
	return key, err
}

//XXX use ccloud code instead?
type config struct {
	Bootstrap_servers string `json:"bootstrap_servers"`
	Security_protocol string `json:"security_protocol"`
	Sasl_mechanisms   string `json:"sasl_mechanisms"`
	Sasl_username     string `json:"sasl_username"`
	Sasl_password     string `json:"sasl_password"`
}

func LoadKafkaConfig() (*kafka.ConfigMap, error) {
	home := os.Getenv("HOME")
	if home == "" {
		return nil, errors.New("No HOME")
	}
	path := filepath.Join(home, ".confluent", "kafka_config.json")
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c config
	err = json.Unmarshal(b, &c)
	return &kafka.ConfigMap{
		"bootstrap.servers": c.Bootstrap_servers,
		"sasl.mechanisms":   c.Sasl_mechanisms,
		"security.protocol": c.Security_protocol,
		"sasl.username":     c.Sasl_username,
		"sasl.password":     c.Sasl_password,
	}, err
}
