package cli

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

type Flags struct {
	Format    string
	Topic     string
	Namespace string
}

type Credentials struct {
	User     string
	Password string
}

func (f *Flags) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&f.Format, "format", "json", "Kafka message format [avro,json]")
	fs.StringVar(&f.Topic, "topic", "", "Kafka topic name")
	fs.StringVar(&f.Namespace, "namespace", "io.brimdata.zync", "Kafka name space for new Avro schemas")
}

func SchemaRegistryEndpoint() (string, Credentials, error) {
	key, err := getKey()
	if err != nil {
		return "", Credentials{}, err
	}
	return key.URL, Credentials{key.User, key.Password}, nil
}

type apiKey struct {
	URL      string `json:"url"`
	User     string `json:"user"`
	Password string `json:"password"`
}

func getKey() (apiKey, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return apiKey{}, err
	}
	path := filepath.Join(home, ".zync", "schema_registry.json")
	b, err := os.ReadFile(path)
	if err != nil {
		return apiKey{}, err
	}
	var key apiKey
	err = json.Unmarshal(b, &key)
	return key, err
}

type config struct {
	BootstrapServers            string `json:"bootstrap_servers"`
	SecurityProtocol            string `json:"security_protocol"`
	SaslMechanisms              string `json:"sasl_mechanisms"`
	SaslUsername                string `json:"sasl_username"`
	SaslPassword                string `json:"sasl_password"`
	TLSClientCertFile           string `json:"tls_client_cert_file"`
	TLSClientKeyFile            string `json:"tls_client_key_file"`
	TLSServerCACertFile         string `json:"tls_server_ca_cert_file"`
	TLSServerInsecureSkipVerify bool   `json:"tls_server_insecure_skip_verify"`
}

func LoadKafkaConfig() ([]kgo.Opt, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	path := filepath.Join(home, ".zync", "kafka.json")
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	var opts []kgo.Opt
	if s := c.BootstrapServers; s != "" {
		opts = append(opts, kgo.SeedBrokers(strings.Split(s, ",")...))
	}
	switch c.SecurityProtocol {
	case "", "PLAINTEXT", "SASL_PLAINTEXT":
	case "SSL", "SASL_SSL":
		var tlsConfig tls.Config
		if c.TLSClientCertFile != "" && c.TLSClientKeyFile != "" {
			cert, err := tls.LoadX509KeyPair(c.TLSClientCertFile, c.TLSClientKeyFile)
			if err != nil {
				return nil, fmt.Errorf("failed to load key pair from tls_client_cert_file and tls_client_key_file: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
		if c.TLSServerCACertFile != "" {
			caCert, err := os.ReadFile(c.TLSServerCACertFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read tls_server_ca_cert_file: %w", err)
			}
			p := x509.NewCertPool()
			if !p.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("failed to append certificates from tls_server_ca_cert_file")
			}
			tlsConfig.RootCAs = p
		}
		tlsConfig.InsecureSkipVerify = c.TLSServerInsecureSkipVerify
		d := &tls.Dialer{
			NetDialer: &net.Dialer{
				Timeout: 10 * time.Second,
			},
			Config: &tlsConfig,
		}
		opts = append(opts, kgo.Dialer(d.DialContext))
	default:
		return nil, fmt.Errorf("unknown security_protocol value %q", c.SecurityProtocol)
	}
	if strings.HasPrefix(c.SecurityProtocol, "SASL_") {
		switch c.SaslMechanisms {
		case "PLAIN":
			a := plain.Auth{
				User: c.SaslUsername,
				Pass: c.SaslPassword,
			}
			opts = append(opts, kgo.SASL(a.AsMechanism()))
		default:
			return nil, fmt.Errorf("unknown sasl_mechanisms value %q", c.SaslMechanisms)
		}
	}
	return opts, nil
}
