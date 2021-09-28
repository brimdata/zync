# zinger

`zinger` is a connector between [Kafka](https://kafka.apache.org/) and
[Zed lakes](https://github.com/brimdata/zed/tree/main/docs/lake).
It can run in either direction, syncing a Kafka topic to a Zed lake data pool
or vice versa.

## Installation

To install `zinger`, clone this repo and run `make install`:
```
git clone https://github.com/brimdata/zinger.git
make -C zinger install
```
Make sure you have Go 1.16 or later installed in your environment and
that your shell path includes Go.

You'll also need `zed` installed to run a Zed lake.  Installation instructions
for `zed` are in the [Zed repository](https://github.com/brimdata/zed).

## Quick Start

For built-in help, run
```
zinger -h
```
Make sure your config files are setup for the Kafka cluster
and schema registry (see below), then run some tests.

List schemas in the registry:
```
zinger ls
```
Create a topic called `MyTopic` with one partition using your Kafka admin tools,
then post some data to a topic:
```
echo '{s:"hello,world"}' | zinger produce -topic MyTopic -
```
See the record you created:
```
zinger consume -topic MyTopic
```
> Hit Ctrl-C to interrupt `zinger consume` as it will wait indefinitely
> for data to arrive on the topic.

In another shell, run a Zed lake service:
```
mkdir scratch
zed lake serve -R scratch
```
Now, sync data from Kafka to a Zed lake:
```
zapi create -orderby kafka.offset:desc PoolA
zinger sync from -topic MyTopic -pool PoolA
```
See the data in the Zed pool:
```
zapi query "from PoolA"
```
Next, create a topic called `MyTarget` with one partition using your Kafka admin tools,
sync data from a Zed pool back to Kafka, and check that it made it:
```
zinger sync to -topic MyTarget -pool PoolA
zinger consume -topic MyTarget
```
Finally, try out shaping.  Put a Zed script in `shape.zed`, e.g.,
```
echo 'value:={upper:to_upper(value.s),words:split(value.s, ",")}' > shape.zed
```
And shape the record from `MyTopic` into a new `PoolB`:
```
zapi create -orderby kafka.offset:desc PoolB
zinger sync from -topic MyTopic -pool PoolB -shaper shape.zed
zapi query -Z "from PoolB"
```

## Configuration

To configure `zinger` to talk to a Kafka cluster and a schema registry,
you must create two files in `$HOME/.zinger`:
[`kafka.json`](kafka.json) and
[`schema_registry.json`](schema_registry.json).

This Kafka config file contains the Kafka bootstrap server
addresses and access credentials.

This schema registry config file contains the URI of the service and
access credentials.

> We currently support just SASL authentication though it will be easy
> to add other authentication options (or no auth).  Please let us know if
> you have a requirement here.

## Description

`zinger` has two sub-commands for synchronizing data:
* `zinger sync from` - syncs data from a Kafka topic to a Zed data pool
* `zinger sync to` - syncs data from a Zed data pool to a Kafka topic

Currently, only the binary
[Kavka/Avro format](https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format)
is supported where the Avro schemas are obtained from a configured
[schema registry]((https://github.com/confluentinc/schema-registry)).

An arbitrary Zed script can be applied to the Zed records in either direction.

The Zed pool used by `zinger` must have its pool key set to `kafka.offset` in
descending order.  `zinger` will detect and report an error if syncing
is attempted using a pool without this configuration.

Each Kafka topic must have a single partition as the system relies upon
the offset to indicate the FIFO order of all records.

### Sync From

`zinger sync from` formats records received from Kafka using the Zed envelope
```
{
  kafka: {topic:string,partition:int64,offset:int64,input_offset:int64},
  key: {...}
  value: {...}
}
```
where the `key` and `value` fields represent the key/value data pair pulled from
Kafka and transcoded from Avro to Zed.

If a Zed script is provided, it is applied to each such record before
syncing the data to the Zed pool.  While the script has access to the
metadata in the `kafka` field, it should not modify these values as this
would cause the synchronization algorithm to fail.

After optionally shaping each record with a Zed script, the data is committed
into the Zed data pool in a transactionally consistent fashion where any and
all data committed by zinger writers has monotonically increasing `kafka.offset`.
If multiple writers attempt to commit to records at the same time containing
overlapping offsets, only one will succeed.  The others will detect the conflict,
recompute the `kafka.offset`'s accounting for the data provided in the
conflicting commit, and retry the commit.

`sync from` records the original input offset in `kafka.input_offset` so when
it comes up, it can query the maximum input offset in the pool and resume
syncing from where it last left off.

To avoid the inefficiencies of write-sharing conflicts and retries,
it is best to configure `zinger` with a single writer per pool.

> Note: the optimisitic locking and retry algorithm is not yet implemented
> and requires a small change to the Zed lake load endpoint.  In the meantime,
> if you run with a single `zinger` writer per pool, this will not be a problem.

### Sync To

`zinger sync to` formats data from a Zed data pool as Avro and "produces"
records that arrive in the pool to the Kafka topic specified.

The synchronization algorithm is very simple: when `sync to` comes up,
it queries the pool for the largest `kafka.offset` present and queries
the Kafka topic for its high-water mark, then it reads, shapes, and
produces all records from the Zed pool at the high-water mark and beyond.

There is currently no logic to detect multiple concurrent writers, so
care must be taken to only run a single `sync to` process at a time
on any given Zed topic.

> Currently, `sync to` exits after syncing to the highest offset.
> We plan to soon modify it so it will run continuously, listening for
> commits to the pool, then push any new to Kafka with minimal latency.

## Debezium Integration

`zinger` can be used with [Debezium](https://debezium.io) to perform database ETL
and replication by syncing Debezium's CDC logs to a Zed data pool with `sync from`,
shaping the logs for a target database schema,
and replicating the shaped CDC logs to a Kafka database
sink connector using `sync to`.

Debezium recommends using a single Kakfa topic for database table.
In this same way, we can scale out the Zed lake and `zinger` processes.

It might be desirable to sync multiple downstream databases with different
schemas to a single upstream database with a unified schema.  This can be
accomplished by having `sync from` read from multiple Kafka topics in parallel
(e,g., reading multiple table formats from different downstream databases),
shape each downstream table accordingly, and store the shaped data in the
unified pool.

A Zed script to shape different schemas to a unified schema is as simple
as a switch statement on the name field of the inbound Kafka topic, e.g.,
```
switch kafka.topic (
  "legacy-oracle-1" => ... ;
  "legacy-oracle-2" => ... ;
  "legacy-mysql-1" => ... ;
  default => ... ;
)
```

> Note that `zinger sync from` does not currently support multiplexing multiple
> inbound topics, but support for this is straightforward and we will add it soon.
>
> We also need to adapt `sync from` so it updates the consumer commit offsets,
> allowing aggressive Kafka retention policies to drop data that has been
> safely replicated into the Zed lake.
