module github.com/brimdata/zinger

go 1.16

require (
	github.com/brimdata/zed v0.30.0
	github.com/confluentinc/confluent-kafka-go v1.7.0 // indirect
	github.com/go-avro/avro v0.0.0-20171219232920-444163702c11
	github.com/riferrei/srclient v0.4.0
	github.com/segmentio/ksuid v1.0.2
	go.uber.org/zap v1.16.0
	gopkg.in/avro.v0 v0.0.0-20171217001914-a730b5802183 // indirect
	gopkg.in/confluentinc/confluent-kafka-go.v1 v1.7.0
)

replace github.com/brimdata/zed => ../zed
