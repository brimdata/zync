package registry

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

type Connection struct {
	*http.Client
	subject string
	servers []string
}

type (
	Schema struct {
		Body string `json:"schema"`
	}
	SchemaVersion struct {
		Subject string `json:"subject"`
		Version int    `json:"version"`
		Schema  string `json:"schema"`
		ID      int    `json:"id"`
	}
	Error struct {
		Message string `json:"message"`
		Code    int    `json:"error_code"`
	}
	Id struct {
		ID int `json:"id"`
	}
)

func NewConnection(subject string, servers []string) *Connection {
	return &Connection{
		Client:  &http.Client{},
		subject: subject,
		servers: servers,
	}
}

func unpack(b []byte, v interface{}) error {
	var e Error
	err := json.Unmarshal(b, &e)
	if err == nil && e.Message != "" {
		return fmt.Errorf("schema registry error code %d: %s", e.Code, e.Message)
	}
	return json.Unmarshal(b, v)
}

func (c *Connection) Lookup(id int) (string, error) {
	path := fmt.Sprintf("/schemas/ids/%d", id)
	b, err := c.get(path, nil)
	if err != nil {
		return "", err
	}
	var s Schema
	err = unpack(b, &s)
	if err != nil {
		return "", err
	}
	return s.Body, nil
}

// body should hold the json encoding of the schema
func (c *Connection) Create(schema []byte) (int, error) {
	// Yo Dawg, I heard you like JSON inside of your JSON.
	// Schema registry wants you to send it the JSON schema
	// definition as a JSON-encoded string of the
	// JSON-encoded schema.
	body, err := json.Marshal(&Schema{Body: string(schema)})
	if err != nil {
		return 0, errors.New("registry couldn't encode schema")
	}
	path := fmt.Sprintf("/subjects/%s/versions", c.subject)
	b, err := c.post(path, body)
	if err != nil {
		return 0, err
	}
	var result Id
	err = unpack(b, &result)
	if err != nil {
		err = errors.New(err.Error() + ": " + string(schema))
		return 0, err
	}
	return result.ID, nil
}

func (c *Connection) List(subject string) ([]int, error) {
	path := fmt.Sprintf("/subjects/%s/versions", c.subject)
	b, err := c.get(path, nil)
	if err != nil {
		return nil, err
	}
	var result []int
	err = unpack(b, &result)
	return result, err
}

func (c *Connection) call(method, path string, body []byte) ([]byte, error) {
	url := "http://" + c.servers[0] + path //XXX
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	return b, err
}

func (c *Connection) post(path string, body []byte) ([]byte, error) {
	return c.call("POST", path, body)
}

func (c *Connection) get(path string, body []byte) ([]byte, error) {
	return c.call("GET", path, body)
}

func (c *Connection) delete(path string, body []byte) ([]byte, error) {
	return c.call("DELETE", path, body)
}
