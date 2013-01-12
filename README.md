# Wukong-Load

This Wukong plugin makes it easy to load data from the command-line
into various data stores.

It is assumed that you will independently deploy and configure each
data store yourself (but see
[Ironfan](http://github.com/infochimps-labs/ironfan)).  Once you've
done that, and once you've written some dataflows with
[Wukong](http://github.com/infochimps-labs/wukong/tree/3.0.0), you can
load them into your data stores with `wu-load`.

Wukong-Load is **not intended for production use**.  It is meant as a
tool to quickly load data into over the command-line, especially
useful when developing flows in concert with wu-local.

## Installation & Setup

Wukong-Load can be installed as a RubyGem:

```
$ sudo gem install wukong-load
```

## Usage

Wukong-Load provides a command-line program `wu-load` you can use to
load data fed in over STDIN.  Get help on `wu-load` by running

```
$ wu-load --help
```

and get help for a specific data store with

```
$ wu-load store_name --help
```

Further details will depend on the data store you're writing to.

### Expected Input

All input to `wu-load` should be newline-separated, JSON-formatted,
hash-like records.  For some data stores, keys in the record may be
interpreted as metadata about the record or about how to route the
record within the data store.

## Elasticsearch Usage

Lets you load JSON-formatted records into an
[Elasticsearch](http://www.elasticsearch.org) database.  See full
options with

```
$ wu-load elasticsearch --help
```

### Connecting

`wu-load` tries to connect to an Elasticsearch server at a default
host (localhost) and port (9200).  You can change these:

```
$ cat data.json | wu-load elasticsearch --host=10.122.123.124 --port=80
```

All queries will be sent to this address.

### Routing

Elasticsearch stores data in several *indices* which each contain
*documents* of various *types*.

`wu-load` loads each document into default index (`wukong`) and type
(`streaming_record`), but you can change these:

```
$ cat data.json | wu-load elasticsearch --host=10.123.123.123 --index=publication --es_type=book
```

A record with an `_index` or `_es_type` field will override these
default settings.  You can change the names of the fields used.

### Creates vs. Updates

If an input document contains a value for the field `_id` then that
value will be as the ID of the record when written, possibly
overwriting a record that already exists -- an update.

You can change the field you use for the Elasticsearch ID property:

```
$ cat data.json | wu-load elasticsearch --host=10.123.123.123 --index=media --es_type=books --id_field="ISBN"
```

## Kafka Usage

Lets you load JSON-formatted records into a
[Kafka](http://kafka.apache.org/) queue.  See full options with

```
$ wu-load kafka --help
```

### Connecting

`wu-load` tries to connect to a Kafka broker at a default host
(localhost) and a port (9092).  You can change these:

```
$ cat data.json | wu-load kafka --host=10.122.123.124 --port=1234
```

All records will be sent to this address.

### Routing

Kafka stores data in several named *queues*.  Each queue can have
several numbered *partitions*.

`wu-load` loads each record into the default queue (`test`) and
partition (0), but you can change these:

```
$ cat data.json | wu-load kafka --host=10.123.123.123 --topic=messages --partition=6
```

A record with a `_topic` or `_partition` field will override these
default settings.  You can change the names of the fields used.
