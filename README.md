# Wukong-Load

This Wukong plugin makes it easy to load data from the command-line
into and out of various data stores.

It is assumed that you will independently deploy and configure each
data store yourself (but see
[Ironfan](http://github.com/infochimps-labs/ironfan)).

<a name="installation">
## Installation & Setup

Wukong-Load can be installed as a RubyGem:

```
$ sudo gem install wukong-load
```

## Usage

<a name="wu-load">
### wu-load

Wukong-Load provides a command-line program `wu-load` you can use to
load data fed in over STDIN into a data store.  Get help on `wu-load`
by running

```
$ wu-load --help
```

and get help for a specific data store with

```
$ wu-load store_name --help
```

All input to `wu-load` should be newline-separated, JSON-formatted,
hash-like records.  For some data stores, keys in the record may be
interpreted as metadata about the record or about how to route the
record within the data store.

Further details will depend on the data store you're writing to.

**Note:** The `wu-laod` program is not designed to handle any
  significant scale of data.  It is only intended as a convenience
  tool for modeling how Wukong dataflows (which **can** scale) will
  interact with data stores.

<a name="wu-ftp>
### wu-ftp

Wukong-Load also provides a program `wu-ftp` which can be used to
mirror and reorganize an FTP/FTPS/SFTP server to local disk.

**Note:** Since FTP is conceptually "single-threaded", no throughput
gains are achieved by having multiple processes try and read the same
file on the same FTP server.  For this reason, `wu-ftp` **can** be
used in a production setting, acting as the fundamental building block
of a more distributed system of which each `wu-ftp` process is
responsible for some files on some FTP servers.

See the <a href="#ftp-usage">FTP usage</a> section below for more
details.

<a name="wu-s3>
### wu-s3

Wukong-Load also provides a program `wu-s3` which can be used to
archive a local directory to S3.

**Note:** S3, like FTP, is conceptually "single-threaded", and no
throughput gains are achieved by having multiple processes try and
read the same key from the same S3 bucket.  For this reason, `wu-s3`
**can** be used in a production setting, acting as the fundamental
building block of a more distributed system of which each `wu-s3`
process is responsible for some keys on some S3 buckets.

See the <a href="#s3-usage">S3 usage</a> section below for more
details.

<a name="elasticsearch-usage">
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

<a name="kafka-usage">
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

<a name="mongodb-usage">
## MongoDB Usage

Lets you load JSON-formatted records into an
[MongoDB](http://www.mongodb.org) database.  See full options with

```
$ wu-load mongodb --help
```

### Connecting

`wu-load` tries to connect to an MongoDB server at a default host
(localhost) and port (27017).  You can change these:

```
$ cat data.json | wu-load mongodb --host=10.122.123.124 --port=1234
```

All queries will be sent to this address.

### Routing

MongoDB stores *documents* in several *databases* which each contain
*collections*.

`wu-load` loads each document into default database (`wukong`) and
collection (`streaming_record`), but you can change these:

```
$ cat data.json | wu-load mongodb --host=10.123.123.123 --database=publication --collection=book
```

A record with a `_database` or `_collection` field will override these
default settings.  You can change the names of the fields used.

### Creates vs. Updates

If an input document contains a value for the field `_id` then that
value will be as the ID of the record when written, possibly
overwriting a record that already exists -- an update.

You can change the field you use for the MongoDB ID property:

```
$ cat data.json | wu-load mongodb --host=10.123.123.123 --database=media --collection=books --id_field="ISBN"
```

<a name="ftp-usage">
## FTP Usage

The program `wu-ftp` is used to pull data down from an FTP server to
local disk.  `wu-ftp` supports several different flavors of
[FTP](http://en.wikipedia.org/wiki/File_Transfer_Protocol), including
[FTPS](http://en.wikipedia.org/wiki/FTPS) and
[SFTP](http://en.wikipedia.org/wiki/SSH_File_Transfer_Protocol), via
the `--protocol` flag.


The [`lftp`](http://lftp.yar.ru/) program is required for `wu-ftp` to
function.

### Specifying FTP protocol, host, and credentials

By default, `wu-ftp` will try to connect anonymously to an FTP server
running on the local machine.  The `--protocol`, `--host`, `--port`,
`--username`, and `--password` options can be used to configure this
behavior.  Here's an example of connecting to a remote FTP server
using a secure FTPS connection:

```
$ wu-ftp --host=ftp.example.com --protocol=ftps --username=bob --password=<password>
```

You can use the `FTP_PASSWORD` environment variable if you don't want
to pass the password on the command-line.

The port is determined automatically from the protocol (e.g. - 21 for
`ftp`, 22 for `sftp`, 443 for `ftps`) but can be explicitly given with
the `--port` flag.

### Specifying locations for local data

Data is downloaded to a local `--output` directory, exactly mirroring
the structure of the remote data on each FTP server.

As each complete file is downloaded, a hardlink is generated in a
local `--links` directory.  The structure of this links directory is

* a lexicographically ordered re-shuffling of the file structure of
the original data
* combined with timestamps
* and a configurable subdirectory root given by the `--name` flag

This allows having multiple invocations of `wu-ftp` share the same
local output and links directories.

### Multiple FTP sources

The `--ftp_sources` setting can be used to specify a collection of
different FTP sources, all to be mirrored locally.

If set, `--ftp_sources` should be a Hash mapping the *name of an FTP
source* to a Hash of the same properties used by `wu-ftp` to process a
single FTP source (`host`, `username`, `protocol`, &c.).

It's easiest to set the `--ftp_sources` flag from within a deploy pack
in a configuration file, as in this example:

```yaml
# in config/settings.yml
---
ftp_sources:
  stock_prices:
    host:     ftp.finance-company.com
	username: bob
	password: hello
	protocol: sftp
  supply_chain:
    host:     ftp.supplies.example.com
	username: mary
	password: goodbye
	protocol: ftp
	ignore_unverified: true
```	

If an `--ftp_sources` setting is present, you can invoke `wu-ftp` on
one of the listed sources by name:

```
$ wu-ftp --output=/tmp/raw --links=/tmp/clean stock_prices
```

<a name=s3-usage">
## S3 Usage

The program `wu-s3` is used to archive data from a local directory to
S3.

The [`s3cmd`](http://s3tools.org/s3cmd) program is required for
`wu-s3` to function.

### Specifying bucket and credentials

By default, `wu-ftp` will adopt whatever the the system credentials
for `s3cmd` are:

```
$ wu-s3 --input=/local/dir --bucket=s3://bucket-name/path/within
```

You can pass the `--s3cmd_config` flag to pass the path to another
s3cmd configuration file, perhaps with different keys than the default
installed on the system.

### Integration with FTP

The `wu-ftp` program can be used to locally mirror the contents of an
FTP server.  This works nicely in combination with `wu-s3` which can
then archive the local contents to S3.  For this reason, `wu-s3` also
reads the `--ftp_sources` setting, just as `wu-ftp` does.

The configuration for `--ftp_sources` should be amended with the
`bucket` setting naming the S3 bucket (and optional path) to archive
data to:

```yaml
# in config/settings.yml
---
ftp_sources:
  stock_prices:
    host:     ftp.finance-company.com
	username: bob
	password: hello
	protocol: sftp
	bucket:   s3://archive.example.com
  supply_chain:
    host:     ftp.supplies.example.com
	username: mary
	password: goodbye
	protocol: ftp
	ignore_unverified: true
	bucket:   s3://archive.example.com
```

If an `--ftp_sources` setting is present, you can invoke `wu-s3` on
one of the listed sources by name:

```
$ wu-s3 --input=/tmp/clean stock_prices
```
