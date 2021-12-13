## Design Thoughts

> This is deprecated by the design outlined in the README.
> We need update the implemented algorithm with cursors to limit the
> scans to only the ranges needed.  After we do this, we will move
> the design from the README back to this file.  See issue #29.

This document describes a proposed Zed-based design for
* syncing one or more Kafka topics to a "raw" data pool in a transactionally
consistent fashion,
* identifying disaggregated "transactions" in the "raw" pool comprising
one or more records with a completion condition,
* applying ETL to the "completed" records in "raw" and storing the ETL'd records in a
"staging" pool,
* alongside all stagings records, in a transactionally consistent fashion,
recording the transaction IDs of completed transactions that have been successfully
ETL'd to staging,
* "expiring" transactions that are never "completed" because of bugs/faults, and
* maintaining a "cursor" of progress so the system may be efficiently scaled up
by processing only the most recent set of records comprising all of the
uncompleted and unexpired records at any given time.

> Zync doesn't quite work as described here right now, but the changes
> required are small and easy.

We will demonstrate the ideas here with some example ZSON files that can be run
by hand with `zed` and a Zed lake to emulate how this would all work.

We envision a sync command that syncs from a "raw" pool to a "staging" pool,
e.g.,
```
zync sync raw staging
```
> TBD: implement `zync sync`


### Data Model

For this discussion, we assume one process listens to one or more Kafka topics
and synchronizes these topics to a the "raw" pool while another process
applies ETL to the "raw" records and stores them in the "staging" pool.
(Another process would sync "staging" to one or more Kafka destination topics to be
synced to a target database but this part is left as an exercise for the reader.)

#### "raw" Data Model

In the example here, all of the data in the "raw" pool has this Zed type signature:
```
{
  seqno: int64,
  kafka: {
    topic: string,
    partition: int64,
    offset: int64,
  },
  value: {
    txn: int64,
    done: bool,
    row: <any record type>,
  }
}
```
In practice, the transaction ID (`txn`) and the `done` condition would be stored
inside of the row data, but here, we separate them for clarity.

The pool key of "raw" is configured to be `seqno` and zync assigns a monotonically
increasing integer as the `seqno` of each record committed to the pool.
This way, all of the records in the "raw" pool are always sorted by `seqno`
and records can be efficiently processed with range scans over `seqno`.

Likewise, `seqno` and the `kafka.offset` can be used to provide transactionally
consistent synchronization between Kafka and the "raw" pool, where the system
is robust to restarts and interruptions by simply coming up and
finding the largest `kafka.offset` for each topic, e.g., by using this Zed query.
```
LakeOffset:=max(seqno),KafkaOffset:=max(kafka.offset) by kafka.topic
```
This query can be made efficient by scanning only beyond the largest offset containing no unprocessed
(or unexpired) records from the "staging" pool (as explained below).  We call this
the _cursor_, which we can use to find the relevant offsets more efficiently as follows:
```
seqno > $cursor | LakeOffset:=max(seqno),KafkaOffset:=max(kafka.offset) by kafka.topic
```
where `$cursor` is the literal value of the cursor derived from "staging".
Note that this range scan is efficient because the pool data is sorted by `seqno`.
Moreover, this query is only needed at startup.  Once the offsets are computed,
they can be maintained in process as the `sync from` algorithm operates.

#### "staging" Data Model

Data committed to the "staging" pool is subsequently synced to one or more Kafka topics.
The Zed type signature of each record stored in "staging" and meant to be synced to
a Kafka topic is:
```
{
  seqno: int64,
  kafka: {
    topic: string,
    partition: int64,
    offset: int64,
  },
  value: <any record type>
}
```
The `kafka.offset` value corresponds to the offset in the indicated topic and
the records in the `value` field are sent in the order dictated by the offset.

The `seqno` field represents the largest sequence number from the "raw" pool
that this record depended upon, i.e., if this output record was generated from
multiple input records comprising a disaggregated transaction, then `seqno` is the
maximum `seqno` of these input records.  Once the system "cursor" exceeds this value,
this record's transaction ID is no longer needed and can be excluded as
a candidate for processing.

The "staging" pool's pool key is `seqno` and is sorted in descending order.

Each commit to the staging pool also records the cursor from the "raw" pool
at the time of the "staging" commit, where the cursor is defined as the
largest "raw" sequence number that contains only processed and expired records.
It is stored with the following type signature:
```
{
  seqno: int64,
} (=cursor)
```
Given this data model, we can efficiently find the largest committed cursor with:
```
is(type(cursor)) | head 1 | cut cursor:=seqno
```
And we can find all of the transaction IDs past the cursor that are already
committed with:
```
seqno > $cursor | cut txn
```
Note that it is an invariant that the committed cursor is always less than or equal
to all of the `seqno` fields in a given commit.

Finally, we can find the maximum offset for each topic using
```
max(kafka.offset) by kafka.topic
```
Since this query is unbounded, it would be more efficient to find the
first offset in descending order then stop when they are all found.

This can be easily accomplished by running
```
kafka.topic==$topic | head 1
```
for each literal-valued `$topic`.

Note that it could be better to run this in parallel with a single scan for
the max offset of each predetermined topic name, which might look something
like this:
```
switch kafka.topic (
  "table1" => head 1 | cut kafka;
  "table2" => head 1 | cut kafka;
  ...
  "tableN" => head 1 | cut kafka;
) | offsets:=collect(kafka) by typeof(this)
```

## Example

We will illustrate this design with a simple example comprising
two input tables received on two Kafka topics being transformed to
one output table.

The input tables are:
* the "order" table with signature `{customer:string,item:string,qty:int64}`
* the "menu" table with signature `{item:string,price:float64}`

The output table is:
* the "order" table with signature `{customerID:int64,,menuID:int64,qty:int64,total:float64}`

> For multiple output tables, we need to work out a multi-record output using "explode"
> for each transactiom budle.

### Setup

To try out this example, run a Zed lake service in one shell:
```
mkdir scratch
zed serve -lake scratch
```
and in another create the ETL pools:
```
zed create -orderby seqno:asc raw
zed create -orderby seqno:desc staging
```

### Example Inputs

Given the above assumptions, suppose the following records are received from
Kafka as each indicated batch (a batch may be terminated by a timeout,
a max number of records, etc. but here we just impose batch boundaries to support
the example) and the ZSON batches here depict the data that `zync` would
create by reading and converting Avro records off the two Kafka topics
"order" and "inventory".

This files are stored in the [`demo`](demo) directory of this repo.

Here is `demo/consumer-1.zson`:
```
{
  seqno: 1,
  kafka: {
    topic: "order",
    offset: 1,
  },
  value: {
    txn: 10000,
    done: false,
    row: {
      customer: "jane",
      product: "taco",
      qty: 2,
    }
  }
}
{
  seqno: 2,
  kafka: {
    topic: "order",
    offset: 2,
  },
  value: {
    txn: 10001,
    done: false,
    row: {
      customer: "bob",
      product: "burrito",
      qty: 1,
    }
  }
}
{
  seqno: 3,
  kafka: {
    topic: "menu",
    offset: 1,
  },
  value: {
    txn: 10000,
    done: true,
    row: {
      product: "taco",
      price: 1.99,
    }
  }
}
```

Load this data into the "raw" pool:
```
zed load -use raw@main demo/consume-1.zson
```

Now we can run this to find all of the disaggregated transactions,
re-aggregate them by `txn` ID, and compute the maximum `seqno` across the bundle:
```
zed query "from raw | records:=collect(this),seqno:=max(seqno),done:=or(value.done) by txn:=value.txn"
```
We want to apply ETL logic to the records that are ready to be processed
(e.g., have `done` true) so we simply reach into the records array to
create our new ETL'd record.  We will put this in a file called `demo/etl.zed`:
```
const customerIDs = |{
        "jane": 1,
        "bob": 2,
        "sarah": 3
}|;
const menuIDs = |{
        "burrito": 100,
        "taco": 200,
        "chips": 300
}|;
from raw
| records:=collect(this),offsets:=max(seqno),done:=or(value.done) by txn:=value.txn
| done==true
| cut this:={
    customerID:customerIDs[records[0].value.row.customer],
    menuID:menuIDs[records[0].value.row.product],
    qty: records[0].value.row.qty,
    total: records[0].value.row.qty * records[1].value.row.price
  }
```
You can see the processed record by running this:
```
zed query -I demo/etl.zed
```
And we get this:
```
{customerID:1,menuID:200,qty:2,total:3.98}
```
`zync sync` would then sync this result --- which here is a single record
but would more generally be multiple records --- by wrapping it in Kafka meta
info and updating the cursor, e.g., as follows:
```
{
  seqno:3,
  kafka:{topic:"order",offset:1},
  row:{customerID:1,menuID:200,qty:2,total:3.98}
}
{seqno:2}(=cursor)
```
The cursor is now at 2 since there still is pending data with
a `seqno` of 2.

> NOTE this encapsulation will automatically run be `zync sync` once
> we add this capability.

To illustrate the steps here, you can manually load this data into
"staging" using:
```
zed load -use staging@main demo/staging-1.zson
```

Next suppose new data arrives that complete the pending one.  Load it with:
```
zed load -use raw@main demo/consume-2.zson
```

Now, suppose zync restarts in this sitaution.  Here are the steps needed to
merge the new transaction into "staging".

First, we need to find the current cursor stored in "staging":
```
zed query "from staging | is(type(cursor)) | max(seqno)"
{max:2}
```
Ok, it's '2'.  Now we get all the transaction data from "raw" greater than 2
and we get:
```
{seqno:2,kafka:{topic:"order",offset:2},value:{txn:10001,done:false,row:{customer:"bob",product:"burrito",qty:1}}}
{seqno:3,kafka:{topic:"menu",offset:1},value:{txn:10000,done:true,row:{product:"taco",price:1.99}}}
{seqno:4,kafka:{topic:"menu",offset:2},value:{txn:10001,done:true,row:{product:"burrito",price:5.99}}}
```
Note that seqno 3 has already been processed.  We can get a list of sequence numbers
already processed from "staging", e.g.,
```
zed query "from staging | not is(type(cursor)) | seqno >= 2 | cut seqno"
```
and we can do an _anti join_ with the "raw" transactions to get just the records
that we want to process.  We'll put this in `demo/update.zed`:
```
from (
  raw => seqno >= 2 | sort seqno;
  staging => not is(type(cursor)) | seqno >= 2 | cut seqno | sort seqno;
)
| anti join on seqno=seqno
```
Run this with
```
zed query -I demo/update.zed
```
And we get exactly the records for the pending transaction:
```
{seqno:2,kafka:{topic:"order",offset:2},value:{txn:10001,done:false,row:{customer:"bob",product:"burrito",qty:1}}}
{seqno:4,kafka:{topic:"menu",offset:2},value:{txn:10001,done:true,row:{product:"burrito",price:5.99}}}
```
Now we can put together the anti-join step with the ETL step in `demo/update-etl.zed`:
```
zed query -I demo/update-etl.zed
{customerID:2,menuID:100,qty:1,total:5.99}
```

`zync sync` would then generate this update for "staging"
```
{
  seqno:4,
  kafka:{topic:"order",offset:2},
  row:{customerID:2,menuID:100,qty:1,total:5.99}
}
{seqno:4}(=cursor)
```
