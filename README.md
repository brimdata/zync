# zync

`zync` is a connector between [Kafka](https://kafka.apache.org/) and
[Zed lakes](https://github.com/brimdata/zed/tree/main/docs/lake).
It can run in either direction, syncing a Kafka topic to a Zed lake data pool
or vice versa.

`zync` can also apply transformations from one or more raw data pools
to a staging data pool in a transactionally consistent fashion based on
Debezium's read/create/update/delete model for CDC database logs.
In this way, you can use `zync` to sync input topics to raw data pools,
apply Debezium-aware transforms from the raw pools to a staging pool,
and use `zync` to sync a staging pool to a target database, typically
a data warehouse.

## Installation

To install `zync`, clone this repo and run `make install`:
```
git clone https://github.com/brimdata/zync.git
make -C zync install
```
Make sure you have **Go 1.16** installed in your environment and
that your shell path includes Go.

**WARNING: Go 1.17 currently does not work with the Kafka Go library as there
is an unresolved bug that is triggered by Kafka's use of cgo.
[See Issue #15](https://github.com/brimdata/zync/issues/15#issuecomment-929210913).**

You'll also need `zed` installed to run a Zed lake.  Installation instructions
for `zed` are in the [Zed repository](https://github.com/brimdata/zed).

## Quick Start

For built-in help, run
```
zync -h
```
Make sure your config files are setup for the Kafka cluster
and schema registry (see below), then run some tests.

List schemas in the registry:
```
zync ls
```
Create a topic called `MyTopic` with one partition using your Kafka admin tools
and, in another window, set up a consumer to display data from that topic:
```
zync consume -topic MyTopic
```
Next, post some data to the topic:
```
echo '{s:"hello,world"}' | zync produce -topic MyTopic -
```
This transforms the ZSON input to Avro and posts it to the topic.
The consumer then converts the Avro back to ZSON and displays it.

> Hit Ctrl-C to interrupt `zync consume` as it will wait indefinitely
> for data to arrive on the topic.

### Syncing to a Zed Lake

In another shell, run a Zed lake service:
```
mkdir scratch
zed lake serve -R scratch
```
Now, in your first shell, sync data from Kafka to a Zed lake:
```
zapi create -orderby seqno PoolA
zync from-kafka -topic MyTopic -pool PoolA
```
See the data in the Zed pool:
```
zapi query "from PoolA"
```
Next, create a topic called `MyTarget` with one partition using your Kafka admin tools,
sync data from a Zed pool back to Kafka, and check that it made it:
```
zync to-kafka -topic MyTarget -pool PoolA
zync consume -topic MyTarget
```
Finally, try out shaping.  Put a Zed script in `shape.zed`, e.g.,
```
echo 'value:={upper:to_upper(value.s),words:split(value.s, ",")}' > shape.zed
```
And shape the record from `MyTopic` into a new `PoolB`:
```
zapi create -orderby kafka.offset PoolB
zync from-kafka -topic MyTopic -pool PoolB -shaper shape.zed
zapi query -Z "from PoolB"
```

## Configuration

To configure `zync` to talk to a Kafka cluster and a schema registry,
you must create two files in `$HOME/.zync`:
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

`zync` has two sub-commands for synchronizing data to and from Kafka:
* `zync to-kafka` - syncs data from a Zed data pool to a Kafka topic
* `zync from-kafka` - syncs data from a Kafka topic to a Zed data pool

Currently, only the binary
[Kavka/Avro format](https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format)
is supported where the Avro schemas are obtained from a configured
[schema registry]((https://github.com/confluentinc/schema-registry)).

An arbitrary Zed script can be applied to the Zed records in either direction.

The Zed pool used by `zync` must have its pool key set to `kafka.offset` in
ascending order.  `zync` will detect and report an error if syncing
is attempted using a pool without this configuration.

### Syncing From Kafka

`zync from-kafka` encapusulates records received from Kafka using the envelope
```
{
  kafka: {topic:string,partition:int64,offset:int64},
  key: {...}
  value: {...}
}
```
where the `key` and `value` fields represent the key/value data pair pulled from
Kafka and transcoded from Avro to Zed and the `kafka` field contains metadata
describing the topic, partition, and offset of the data received from Kafka.

If a Zed script is provided, it is applied to each such record before
syncing the data to the Zed pool.  While the script has access to the
metadata in the `kafka` field, it should not modify these values as
`zync` relies on this field.

After optionally shaping each record with a Zed script, the data is committed
into the Zed data pool in a transactionally consistent fashion where any and
all data committed by `zync` writers must have monotonically increasing `kafka.offset`
relative to each topic indicated in `kafka.topic`.

As the Kafka topic and offset is stored in each record,
the `zync from-kafka` command can query the maximum input offset in the pool
for each topic and resume syncing from where it last left off.

To avoid duplicate records,
it is best to configure `zync` with a single writer per Kafka topic.

> Note: we currently do not detect multiple writers to a pool but can do
> so with a small change to the load API to track commit IDs and detect
> write conflicts when the writer is not writing to the head commit that
> it expects.

### Syncing To Kafka

`zync to-kafka` reads records that arrive in a Zed pool, transcodes them
to Avro, and "produces" them to the Kafka topic specified in the
`kafka` metadata field of each record.

The synchronization algorithm is very simple: when `zync to-kafka` starts up,
it queries the pool for the largest `kafka.offset` for each `kafka.topic`
present and queries
each Kafka topic for its high-water mark.  Then it reads, shapes, and
produces all records from the Zed pool at the high-water mark and beyond
for each topic.

There is currently no logic to detect multiple concurrent writers to the
same Kafka output topic, so
care must be taken to only run a single `zync to-kafka` process at a time
for any given Kafka topic.

> Note: `zync to-kafka` currently exits after syncing to the highest contiguous offset.
> We plan to soon modify it so it will run continuously, listening for
> commits to the pool, then push any new to Kafka with minimal latency.

## Debezium Integration

`zync` can be used with [Debezium](https://debezium.io) to perform database ETL
and replication by syncing Debezium's CDC logs to a Zed data pool with `zync from-kafka`,
shaping the logs for a target database schema using an experimental `zync etl`
command, and replicating the shaped CDC logs to a Kafka database
sink connector using `zync to-kafka`.

The goal of `zync etl` is to do sophisticated ETL that may involve the denormalization
of multiple tables into one.

The model here is that `zync etl` processes data from an input pool to an output
pool where `zync from-kafka` is populating the input pool and `zync to-kafka` is processing
the output pool.

Each Kafka topic must have a single partition as Debezium relies upon
the Kafka offset to indicate the FIFO order of all records.

### Design assumptions

Debezium recommends using a single Kakfa topic for database table.
In this same way, we can scale out the Zed lake and `zync` processes.

We assume the following Debezium event types:
* `r` events indicate the read of a row during the startup snapshot,
* `c` events indicate the creation of a new row,
* `u` events indicate the update of an existing row, and
* `d` events indicate the deletion of an existing row.

To perform denormalization, we need to collect multiple input events to
produce a single output event.  For example, a `c` event on input table A
and another `c` event on table B might need to be coalesced into a
mixed `c` event on output table C.  There must be some way to correlate
such events, e.g., by presuming there is a unique ID present in both records,
or a foreign key in record A that can be used to locate
the primary key in record B.

Also, it might be desirable for a `u` event on table A to wait for a
`u` event on table B to perform a denormalized `u` event on table C
where the ETL needs info from both the A update and the B update to produce the
combined C update.
In other cases,
it might be more straightforward to simply allow the individual updates to
be transformed to table C updates.   This would mean the ETL steps should be
specified in pieces so the `zync etl` command could apply a single configuration
to a combined A/B update versus a single A or a single B update.

The `zync etl` command assumes that each input record participates in exactly
one transformation event.  This way, `zync etl` can track which input records have
been processed and which remain to be processed by recording in the transformed
pool all the Kafka offsets by topic, exactly once, of each input event processed.

Also, for now, we will assume that prior to processing by the pipeline ETLs,
the `zync etl` command pre-maps each
Debezium event into the form
```
{
  kafka: {topic:string,partition:int64,offset:int64},
  op: string,
  key: null
  value: {...}
}
```
where `op` is the Debezium event type, i.e., one of "r", "c", "u", or "d",
and value is the `after` field from the Debezium event.

Internally, the code applies this Zed when reading events from the input/"raw" pool:
```
... | cut kafka,op:=value.op,key:=null,value:=value.after | ...
```

> Note that with this approach, we do not have to configured the Kafka sink
> to unwrap the Debezium events for vanilla consumption by the JDBC sink.
> Instead we can format events into the staging pool that have the form described
> in the [Confluence JDBC sink documentation](https://docs.confluent.io/kafka-connect-jdbc/current/sink-connector/index.html).

### `zync etl` configuration

The `zync etl` command syncs one Zed data pool to another where the
input pool is created with `zync from-kafka` and the output pool is formatted
for `zync to-kafka`.

Since configuration can be complex with multiple ETLs of varying types,
the command is configured with YAML.

> These YAML options are preliminary and will be iterated upon and change.

Configuration is currently all in a single YAML file.

```
# Routes define a pool for each Kafka topic, whether it's an input or an output.
# The namespace of topics is currently shared between inputs and outputs as we
# presume a single Kafka cluster for both the inputs and outputs (though this is
# not a strict requirement).
inputs:
  - topic: TableA
    pool: Raw
  - topic: TableB
    pool: Raw

outputs:    
  - topic: TableC
    pool: Staging

# Transforms define rules from one or more input tables to
# to an output table, using the routes to determine the pools where each
# topic is stored.
transforms:
  - type: denorm // denorm or stateless
    where: |
      optional Zed Boolean expression to select this rule
    left: TableA
    right: TableB
    join-on: TableA.ID=TableB.InvoiceID
    out: TableC
    zed: |
      Zed script that is applied to all records before storing them in the pool
      and creates the top-level field this.TableC
  - type: stateless
    where: value.op=="u"
    in: TableA
    out: TableC
    zed: |
      Zed script that is applied to all records before storing them in the pool
      and creates the top-level field this.TableC
```

> Note that this YAML design is only configuring a single ETL pipeline between
> Zed data pools without any Kafka integration.  We need to work out another layer of
> YAML config that can embed these ETL configurations and additional logic
> to wire up the `zync from-kafka` and `zync to-kafka` processes and run many instances
> over a cluster.  The current plan is to have a `zync build` command that will take
> the `zync` YAMLs and produce `helm` charts to deploy all the needed `zync` processes
> across a Kubernetes cluster.

### The ETL Algorithm

The algorithm here describes how the ETLs are stitched together to perform the
desired transformations.  Note that the YAML config above knows nothing
about the details below.  However, it's useful to know how things work so you
can debug problems that arise (and perhaps performance issues).

> TBD: We'll create a library of Brim queries that can be used to easily
> navigate to different views of what's going on in a live ETL process.

When the `zync etl` command is run, it queries the current state of its
input and output pools, determines if any of the ETLs have work to do,
runs the ETLs to get the transformed results, creates completion records
for all records processed (by each input topic),
and records the transformed records and completions records in an atomic
commit in the output pool.  It then exits.

To make incremental updates efficient, the pools must be sorted by `kafka.offset`
(in ascending order) and
for each topic, we maintain a cursor per input topic,
referred to below as `$cursor[$topic]`.

A completion record is recorded in the output pool for each input record that has
been processed, which has the form
```
{kafka:{topic:string,offset:int64}}(=done)
```
> Note there is no Kafka partition as we require in-order delivery and thus
> only one partition per topic.

At startup, to compute the cursors we simply run a query for each input topic
on the output lake
```
is(type(done)) | max(kafka.offset) by kafka.topic
```
> We can make this efficient by using `head 1` inside of switch legs where each
> switch case is one of the topics and scanning in descending order, which is the
> reverse order of the pool.  TBD: we have an issue to make reverse range scanning
> efficient; right now, we read the whole range and do a sort -r.  This is not
> a trivial task but isn't too hard.

We can then enumerate the unprocessed records, by scanning the raw pool
from the smallest cursor up and doing an anti join for each topic.

The following  pseudo Zed would be stitched together from the YAML config by `zync`
(assuming two input topics, "TableA" and "TableB").
```
split (
    => from (
        Raw range from $cursor["TableA"] to MAXINT64 => kafka.topic=="TableA";
        Staging range from $cursor["TableA"] to MAXINT64 => is(type(done)) && kafka.topic=="TableA";
      ) | anti join on kafka.offset=kafka.offset;
    => from (
        Raw range from $cursor["TableB"] to MAXINT64 => kafka.topic=="TableB";
        Staging range from $cursor["TableB"] to MAXINT64 => is(type(done)) && kafka.topic=="TableB";
      ) | anti join on kafka.offset=kafka.offset;
  )
  | switch (
    <where-denorm> =>
      split (
        => kafka.topic==<left> | this[<left>]:=value.after | sort <right-key>;
        => kafka.topic==<right> | this[<right>]:=value.after | sort <left-key>;
      )
      | join on <join-on>
      | <zed>
      | value.after:=this[<out>]
      | kafka.topic:=<out> // zync will fix kafka.offset
      ;
    <where-stateless> && kafka.topic==<in> =>
      | <zed>
      | value.after:=this[<out>]
      | kafka.topic:=<out> // zync will fix kafka.offset
      ;
    ...
  )
```

### Demo

Start a Zed lake service.
```
mkdir scratch
zed lake serve -R scratch
```
Create `Raw` and `Staging` pools:
```
zapi create -orderby kafka.offset Raw
zapi create -orderby kafka.offset Staging
```
Load the first batch of test data into `Raw`, as if `zync from-kafka` imported
it from its topics to `Raw` as Debezium CDC logs:
```
zapi load -use Raw@main demo/batch-1.zson
```
You can easily see the Debezium table updates loaded into `Raw` with `zapi query`:
```
zapi query -f table "from Raw | kafka.topic=='Invoices' | this:=value.after"
zapi query -f table "from Raw | kafka.topic=='InvoiceStatus' | this:=value.after"
```
These are all type `r` (read) Debezium logs and represent two new rows in each
of the `Invoice` and `InvoiceStatus` tables.  Transform them to `Staging` with
```
zync etl demo/invoices.yaml
```
This will report the commit ID and number of input records processed.
Note that the number of records committed into
the pool is different than the number of records produced by ETL as the destination
pool includes metadata records tracking which input events have been processed.

After running the ETL, you can see the denormalized CDC updates in the
`Staging` pool:
```
zapi query -f table "from Staging | kafka.topic=='NewInvoices' | this:=value.after"
```
You can also see the progress updates marking the input records completed
that are stored alongside the data in `Staging`:
```
zapi query "from Staging | is(type(done))"
```

If you run the ETL again with no new data, it will do nothing as you do not
want duplicate data in the output:
```
zync etl demo/invoices.yaml
```
> `zync` uses an anti join between the completion records in the output pool
> and the input records to remove from the input all records that have already
> been processed.

Now suppose new data arrives from Debezium over Kafka.  Let's load it into
the `Raw` pool:
```
zapi load -use Raw@main demo/batch-2.zson
```
In this file, there are new `Invoices` rows for Charlie and Dan but only an
`InvoiceStatus` row for Charlie.  This means only the Charlie data can be
denormalized and the Dan `Invoices` row will be left unprocessed awaiting
the arrival of its `InvoiceStatus` counterpart.
```
zync etl demo/invoices.yaml
```
You can see that the Charlie row made it Staging:
```
zapi query -f table "from Staging | kafka.topic=='NewInvoices' | this:=value.after"
```
but the Dan row is still pending.  You can see the pending records for this
example by running
```
zapi query -Z -I demo/pending.zed
```
Now let's load another batch of records that provides the InvoiceStatus create
event for the Dan row and a "stateless" `InvoiceStatus` update to change
Alice's status to "paid":
```
zapi load -use Raw@main demo/batch-3.zson
zync etl demo/invoices.yaml
```
Now we can see the Dan row made it to `Staging`:
```
zapi query -f table "from Staging | not is(type(done)) | this:={seqno:kafka.offset,op:value.op,row:value.after} | fuse"
```

> NOTE: We formatted this output a bit differently as the updates are getting
> more complex.  Here we numbered each update according to CDC order,
> included the Debezium `op` field, and fused the tables so you can see where
> the updates fall in the table.

Finally, in the last batch, the remaining invoices marked "pending" are
all updated.

```
zapi load -use Raw@main demo/batch-4.zson
zync etl demo/invoices.yaml
```

And re-run the table query from above to see the final result:
```
zapi query -f table "from Staging | not is(type(done)) | this:={seqno:kafka.offset,op:value.op,row:value.after} | fuse"
```


#### `anti join`

If you're curious how anti join works, try this:
```
echo '{a:1,id:1}{a:2,id:2}{a:3,id:2}{a:4,id:3}{a:5,id:4}{a:5,id:5}' > in.zson
echo '{drop:2}{drop:5}' > drop.zson
zq 'anti join on id=drop' in.zson drop.zson
```
