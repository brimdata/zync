# zinger
Zeek/ZNG transcoding gateway for Kafka/Avro

Zinger is a tool for interconnecting Zeek with
[Kafka/Avro](https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format)
via the
[Kafka schema Registry]((https://github.com/confluentinc/schema-registry)).

It uses [kafka sarama](https://github.com/Shopify/sarama) to communicate with
Kafka and [go-avro](https://github.com/go-avro/avro) to build schemas.

Zinger has native support for communicating with the
[Schema Registry REST API](https://github.com/confluentinc/schema-registry) and
for transcoding [Zeek/Zng](https://github.com/mccanne/zq/blob/master/pkg/zng/docs/spec.md)
into [Apache Avro](https://avro.apache.org/).

## Installation

To install zinger, clone this repo and run
```
make install
```
Make sure you have Go installed in your environment and that GOPATH is
in your shell path.

## Usage

For built-in help, run
```
zinger help
```

### zinger listen

The primary use case for zinger is to run it as a server that listens
for incoming connections containing a stream of data in any format supported
by zq, i.e., Zeek TSV streams, ZNG streams, NDJSON, etc.

For example, when Zeek can be run with the
[TSV streaming pluging](https://github.com/looky-cloud/zson-http-plugin)
configured to point at zinger and zinger will transcode all incoming data
onto Kafka/Avro.

For example, running this command
```
zinger -k 192.168.1.1:9092 -r 192.168.1.1:8081 -t zeekdata -s zinger -n com.acme listen -l :6755
```
starts up a process to listen for incoming connections on port `6755` converting
all such streams to Kafka/Avro streams by sending them as a Kafka producer to the
Kafka service at `192.168.1.1:9092`.  The Schema Registry service at
`192.168.1.1:8081` is used to create new schemas based on the incoming data.
All newly created schemas are all created under the namespace `com.acme` under
the subject `zinger`.
Streams are produced to Kafka on the topic `zeekdata`.

### zinger post

Once running, you can test zinger by curling Zeek logs or other zq data
into zinger, or more easily, by running the post subcommand from another
instance of zinger.

For example, to post a log into zinger,
run this command on the same host where `zinger listen` is running:
```
zinger post -a localhost:6755 conn.log
```
This transmits the data in conn.log to zinger, which in turn, converts the data
to Kafka/Avro and publishes it to the configured topic.  If a new schemas needs
to be created to handle the data in conn.log, it will be automatically created
and stored in the Schema Registry.  If the schema already exists in the registry,
the existing schemas will be used.

### zinger ls

To display the schemas in use, run
```
zinger ls
```
Use `-l`, to display each full schema as NDJSON.

## Caveats

This code is in a proof-of-concept stage but with a bit more effort,
could be made production qualities.

Here are a few caveats:
* No SSL or auth.
* The kafka producer writes synchronously; we would need to change this
to async to get decent performance.
* Everything is written to a single topic.  It would be straightforward to add
config to route data to different topics based on simple filter rules
(we could use zql filtering logic and the zq filter proc).
* Crash recoverable restart could be achieved by tying ZNG control acks
to kafka message success so the Zeek TSV plugin would know where to restart.
