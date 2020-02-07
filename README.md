# zinger

Zinger is a receiver for [Zeek](https://www.zeek.org/) logs. It receives logs in any format
supported by [zq](https://github.com/mccanne/zq/) and can store,
process, and forward them to various outputs depending on its configuration.

Currently supported outputs are:

- the **File** output, a simple file writer that writes each incoming log a separate file (in any zq-supported format)
- the **Kafka** output, which interconnects Zeek and
[Kafka/Avro](https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format)
by transcoding Zeek log streams into Avro and storing the Avro schemas
in the Kafka
[Schema Registry]((https://github.com/confluentinc/schema-registry)).

Internally, the Kafka output uses
[Sarama](https://github.com/Shopify/sarama) to communicate with Kafka
and [go-avro](https://github.com/go-avro/avro) to construct
schemas. It has native support for communicating with the Kafka
[Schema Registry](https://github.com/confluentinc/schema-registry) and
for transcoding
[Zeek/Zng](https://github.com/mccanne/zq/blob/master/pkg/zng/docs/spec.md)
into [Apache Avro](https://avro.apache.org/).

## Installation

To install zinger, clone this repo and run `make install`:
```
git clone https://github.com/brimsec/zinger.git
cd zinger
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
[TSV streaming plugin](https://github.com/looky-cloud/zeek-tsv-http-plugin)
configured to point at zinger and zinger will transcode all incoming data
onto Kafka/Avro.

For example, running this command
```
zinger -k -b 192.168.1.1:9092 -r 192.168.1.1:8081 -t zeekdata -s zinger -n com.acme listen -l :6755
```
starts up a process to listen for incoming connections on port `6755` converting
all such streams to Kafka/Avro streams by sending them as a Kafka producer to the
Kafka service at `192.168.1.1:9092`.  The Schema Registry service at
`192.168.1.1:8081` is used to create new schemas based on the incoming data.
All newly created schemas are all created under the namespace `com.acme` under
the subject `zinger`.
Transcoded stream data is transmitted to Kafka on the topic `zeekdata`.

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

This code is in a proof-of-concept stage, but with a bit more effort,
we could make it production quality.

Here are a few caveats:
* No SSL or auth.
* The kafka producer writes synchronously; we would need to change this
to async to get decent performance.
* Everything is written to a single topic.  It would be straightforward to add
config to route data to different topics based on simple filter rules
(we could use zql filtering logic and the zq filter proc).
* Crash recoverable restart could be achieved by tying ZNG control acks
to kafka message success so the Zeek TSV plugin would know where to restart.
* Timestamps are converted to the logical microsecond timestamp documented
in the avro spec.  It appears there are nanosecond timestamps using logical types
documented elsewhere.
* Likewise, IP address and subnets are converted to strings and it seems like
logical types would be preferred.
