# zinger

`zinger` is a connector between Kafka topics and Zed lakes.
It can run in either direction: syncing a Zed lake to a Kafka topic or
syncing a Kafka topic to a Zed lake.

Currently, only the binary
[Kavka/Avro format](https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format)
is supported where the Avro schemas are obtained from a configured
[schema registry]((https://github.com/confluentinc/schema-registry)).

An arbitrary Zed script can be applied to the Zed records in either direction.

Zinger formats records received from Kafka using the Zed envelope:
```
{
        kafka: {topic:string,offset:int64,partition:int32},
        record: {...}
}
```
where `...` indicates the Zed record that results from the configured Zed script
applied to the decoded Avro record.
If there is no such script, the record is simply the verbatim result obtained
from decoding Avro into Zed.

By including the Kafka offset in the Zed records, `zinger` can query the Zed
lake for the largest offset seen and resume synchronization in a reliable and
consistent fashion.

## Installation

To install `zinger`, clone this repo and run `make install`:
```
git clone https://github.com/brimdata/zinger.git
cd zinger
make install
```
Make sure you have Go installed in your environment and that GOPATH is
in your shell path.

## Usage

For built-in help, run
```
zinger -h
```
