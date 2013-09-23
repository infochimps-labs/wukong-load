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
load data fed in over STDIN into a data store.  It's designed to work
effectively with `wu-local` and `wu-dump` as part of UNIX pipelines.

Get help on `wu-load` by running

```
$ wu-load --help
```

All input to `wu-load` should consist of newline-separated records
over STDIN.  For some data stores, JSON-formatted, Hash-like input is
expected.  Keys in the record may be interpreted as metadata about the
record or about how to route the record within the data store.

Here are some quick examples which do what you think they do:

```
$ echo 'an arbitrary line of text' | wu-load kafka --topic=foo

$ echo '{"this": "record", "will": { "be": "indexed"}} | wu-load elasticsearch --index=foo --es_type=bar
$ echo '{"this": "record", "will": { "be": "updated"}, "_id": "existing_id"}' | wu-load elasticsearch --index=foo --es_type=bar

$ echo '{"this": "record", "will": { "be": "indexed"}} | wu-load mongodb --database=foo --collection=bar
$ echo '{"this": "record", "will": { "be": "upserted"}, "_id": "existing_id"}' | wu-load mongodb --database=foo --collection=bar

$ echo '{"this": "record", "is": "indexed"} | wu-load sql --database=foo --table=bar
$ echo '{"this": "record", "is": "upserted"}, "_id": "existing_id"}' | wu-load sql --database=foo --table=bar
```

Further details and options will depend on the data store you're
writing to.  See more information on each specific data store below.
You can also get help for a specific data store with:

```
$ wu-load STORE_TYPE --help
```

**Note:** The `wu-load` program is not designed to handle any
  significant scale of data.  It is only intended as a convenience
  tool for modeling how Wukong dataflows (which **can** scale) will
  interact with data stores.

<a name="wu-dump>
### wu-dump

Wukong-Load provides a command-line program `wu-dump` you can use to
dump data from a data store to STDOUT.  It's designed to work
effectively with `wu-local` and `wu-load` in UNIX pipelines.

Get help on `wu-dump` by running

```
wu-dump --help
```

All output from `wu-dump` will be newline-separated records over
STDOUT.  For some data stores, JSON-formatted, Hash-like output is
produced.

Here are some quick examples which do what you think they do:

```
$ wu-dump kafka --topic=foo
line 1
line 2
line 3
...

$ wu-dump file --input=/data/foo.tsv
foo	a
foo	b
foo	c
...
$ wu-dump file --input=/data/bar.tsv.gz
bar	x
bar	y
bar	z
...

$ wu-dump file --input=data.zip
data.zip	1	foo	a
data.zip	2	foo	b
...
data.zip	1	bar	x
data.zip	2	bar	y
...

$ wu-dump directory --input=/data
/data/foo.tsv	1	foo	a
/data/foo.tsv	2	foo	b
...
/data/bar.tsv.gz	1	bar	x
/data/bar.tsv.gz	2	bar	y
...

```

Further details and options will depend on the data store you're
reading from.  See more information on each specific data store below.
You can also get help for a specific data store with:

```
$ wu-dump STORE_TYPE --help
```

**Note:** The `wu-load` program is not designed to handle any
  significant scale of data.  It is only intended as a convenience
  tool for modeling how Wukong dataflows (which **can** scale) will
  interact with data stores.

<a name="wu-sync>
### wu-sync

Wukong-Load also provides a program `wu-sync` which can be used to
sync data between filesystem like data stores.

`wu-sync` itself provides several types of syncs that can be chained
together to create transactional, scalable processing for batch files.

Here's an example which syncs data from an FTP server to S3, providing
a per-file transactional guarantee throughout:

```
$ wu-sync ftp --host=ftp.example.com --path=/remote/data --output=/data/incoming
$ wu-sync prepare --input=/data/incoming --output=/data/received
$ wu-sync prepare --input=/data/incoming --output=/data/received
$ wu-sync s3 --input=/data/received --bucket=s3://example.com/data
```

Further details and options will depend on the type of sync being
performed.  See more information on each specific sync below.  You can
also get help for a specific sync with:

```
$ wu-sync SYNC_TYPE --help
```

<a name="kafka-usage">
## Kafka Usage

[Kafka](http://kafka.apache.org/) uses the concept of "topics" which
contain numbered "partitions" that contain a sequence of events
concatenated together.  Producers (writers) to Kafka dump in events
and consumers (readers) request data between particular byte offsets
on particular partitions of particular topics.  Kafka's data model is
very simple (everything is a sequence of bytes) so it can store any
kind of data.

Get options for loading data from or dumping data to Kafka:

```
$ wu-load kafka --help
$ wu-dump kafka --help
```

### Connecting

`wu-load` and `wu-dump` try to connect to a Kafka broker at a default
host (localhost) and a port (9092).  You can change this:

```
$ cat data.txt | wu-load kafka --host=10.122.123.124 --port=1234
$ wu-dump kafka --host=10.122.123.124 --port=1234
```

### Routing

`wu-load` and `wu-dump` both assume a default topic (`wukong`) and
partition (0), but you can change these:

```
$ cat data.txt | wu-load kafka --topic=messages --partition=6
$ wu-dump kafka --topic=messages --partition=6
```

### Producing

Writing data to Kafka is simple: pick a topic and partition and push
in some bytes:

```
$ cat data.txt | wu-load kafka --topic=messages
```

To achieve higher throughput (though, remember, `wu-dump` and
`wu-load` are **not** designed for significant load), set the
`--batch_size` option up a bit:

```
$ cat data.txt | wu-load kafka --topic=messages --batch_size=1000
```

### Consuming

Consuming data from Kafka requires picking a topic, partition, and a
byte-offset.

It's rare to want to choose a specific byte-offset (typically only
occurs in delicate situations that no one wants to be in) but you can
do it easily:

```
$ wu-dump kafka --topic=foo --offset=65536
```

The usual approach is to consume either from the beginning or end of
available data on a topic & partition.  `wu-dump`'s default behavior
is to start from the end of a topic:

```
$ wu-dump kafka --topic=foo
```

but you can also start from the beginning:

```
$ wu-dump kafka --topic=foo --from_beginning
```

Another common pattern among Kafka consumers is to choose an offset
that is "wherever you last left off".  Kafka does not provide a
mechansim to remember per-consumer last-offsets so it's up to
consumers to do this for themselves.  Since `wu-dump` is not designed
to run continuously, it does not store "wherever you last left off".

<a name="elasticsearch-usage">
## Elasticsearch Usage

**Note::** ElasticSearch support for `wu-dump` is not currently
  implemented.

[ElasticSearch](http://www.elasticsearch.org) uses the concept of
"indices" which contain "types" to store "documents", each of which is
a schema-less, Hash-like structure.  Every document must also have an
ID which can be generated by ElasticSearch at index (write) time.

`wu-load`, like many tools that interact with ElasticSearch, uses
JSON-serialization.  See full options with

```
$ wu-load elasticsearch --help
```

### Connecting

`wu-load` tries to connect to an Elasticsearch server at a default
host (localhost) and port (9200).  You can change these:

```
$ cat data.json | wu-load elasticsearch --host=10.122.123.124 --port=80
```

### Routing

`wu-load` loads each document into default index (`wukong`) and type
(`streaming_record`), but you can change these:

```
$ cat data.json | wu-load elasticsearch --index=publication --es_type=book
```

A record with an `_index` or `_type` field will override these default
settings.  You can change the names of the fields used with the
`--index_field` and `--es_type_field` options:

```
$ cat data.json | wu-load elasticsearch --index=industry --es_type=publication --index_field=publisher --es_type_field=classification
```

#### Creates vs. Updates

If an input record contains a value for the field `_id` then that
value will be as the ID of the document when written, possibly
overwriting a document that already exists -- an update.

You can change the field you use for the Elasticsearch ID property:

```
$ cat data.json | wu-load elasticsearch --index=media --es_type=books --id_field="ISBN"
```

<a name="mongodb-usage">
## MongoDB Usage

**Note::** MongoDB support for `wu-dump` is not currently implemented.

[MongoDB](http://www.mongodb.org) uses the concept of "databases"
which contain "collections" to store "documents", each of which is a
schema-less, Hash-like structure.  Every document must also have an ID
which can be generated by MongoDB at insert (write) time.

`wu-load`, like many tools that interact with MongoDB, uses
JSON-serialization.  See full options with

```
$ wu-load mongodb --help
```

### Connecting

`wu-load` tries to connect to an MongoDB server at a default host
(localhost) and port (27017).  You can change these:

```
$ cat data.json | wu-load mongodb --host=10.122.123.124 --port=1234
```

### Routing

`wu-load` loads each document into default database (`wukong`) and
collection (`streaming_record`), but you can change these:

```
$ cat data.json | wu-load mongodb --database=publication --collection=book
```

A record with a `_database` or `_collection` field will override these
default settings.  You can change the names of the fields used with
the `--database_field` and `--collection_field` options:

```
$ cat data.json | wu-load mongodb --database=industry --collection=publication --database_field=publisher --collection_field=classification
```

### Creates vs. Updates

If an input record contains a value for the field `_id` then that
value will be as the ID of the document when written, possibly
overwriting a document that already exists -- an update.

You can change the field you use for the MongoDB ID property:

```
$ cat data.json | wu-load mongodb --database=publication --collection=books --id_field="ISBN"
```

<a name="sync-usage">
## Syncing

Syncing data between filesystems is always challenging to do well
though there are many good tools available.

In a Big Data setting, syncs are even more challenging because of the
size of the files involved which exacerbates problems of blocking,
atomicity, transactionality, throughput, memory, &c.  The best
approach is to split the process up into several steps, each of which
does one simple operation robustly, correctly, and in a way that can
be tracked.

The `wu-sync` provides several types of syncs that can be used
together to create transactional, scalable processing for batch files.
Each `wu-sync` command is meant to be run on a regular schedule every
few minutes, perhaps via [`cron`](http://en.wikipedia.org/wiki/Cron).

All of the `wu-sync` sync types accept the `--dry_run` option which
will go through the sync showing what would be done but not doing it.

### From FTP

The `ftp` sync will sync data from an FTP server to a local disk.  It
supports several different flavors of
[FTP](http://en.wikipedia.org/wiki/File_Transfer_Protocol), including
[FTPS](http://en.wikipedia.org/wiki/FTPS) and
[SFTP](http://en.wikipedia.org/wiki/SSH_File_Transfer_Protocol).

On each invocation, `wu-sync ftp` will download any files (or parts of
files) present under `--path` on the remote server but not present in
the local `--output` directory.  Files present on the local filesystem
but not on the remote server will be ignored.

The [`lftp`](http://lftp.yar.ru/) program is required for `wu-ftp` to
function.

Get general help on `wu-sync ftp` with

```
$ wu-sync ftp --help
```

**Note:** Since FTP is conceptually "single-threaded", no throughput
gains are achieved by having multiple processes try and read the same
file on the same FTP server.  For this reason, `wu-sync ftp` **can**
be used in a production setting, acting as the fundamental building
block of a more distributed system of which each `wu-sync ftp` process
is responsible for some files on some FTP servers.

#### Specifying FTP protocol, host, and credentials

By default, `wu-sync ftp` will try to connect anonymously to an FTP
server running on the local machine.  The `--protocol`, `--host`,
`--port`, `--username`, and `--password` options can be used to
configure this behavior.  Here's an example of connecting to a remote
FTP server using a secure FTPS connection:

```
$ wu-sync ftp --host=ftp.example.com --protocol=ftps --username=bob --password=<password> --output=/data/ftp
```

You can use the `FTP_PASSWORD` environment variable if you don't want
to pass the password on the command-line.

The port is determined automatically from the protocol (e.g. - 21 for
`ftp`, 22 for `sftp`, 443 for `ftps`) but can be explicitly given with
the `--port` flag.

### Preparing Files for Downstream Consumption

One of the immediate issues that arises when syncing files is that it
may not be clear to a client when the file on the remote server the
client is connected to has finished uploading.  It may also not be
clear if the file has finished downloading to the local filesystem.

The approach taken by Wukong is to run a separate "archival" step
which syncs data in a local `--input` directory with data in a local
`--output` directory.  Files will only show up in the `--output`
directory when they are complete, as measured by them having **stopped
growing** in the `--input` directory.

`wu-sync` is not a continuously running process, however, and so the
only way for it to reliably know when files have stopped growing is
for it to store file sizes from one invocation in order to compare
them to file sizes on the next invocation.  For this reason, creating
files in the `--output` directory requires at **at least two**
invocations of `wu-sync` (with the same set of parameters).

Here's an example of the `--input` directory `/data/ftp`:

```
/data/ftp
├──   alice
│   └──   project_1
│       └──   file_1
├──   bob
│   ├──   project_1
│   │   ├──   file_1
│   │   ├──   file_2
│   │   └──   file_3
│   └──   project_2
│       ├──   file_1
│       └──   file_2
└──   README
```

After running `wu-sync prepare` twice

```
$ wu-sync prepare --input=/data/ftp --output=/data/clean
$ wu-sync prepare --input=/data/ftp --output=/data/clean
```

the `--output` directory `/data/clean` should look exactly the same
as the input directory:

```
/data/clean
├──   alice
│   └──   project_1
│       └──   file_1
├──   bob
│   ├──   project_1
│   │   ├──   file_1
│   │   ├──   file_2
│   │   └──   file_3
│   └──   project_2
│       ├──   file_1
│       └──   file_2
└──   README
```

Nothing here seems magical, but the log output of `wu-sync prepare`
reveals a lot that's going on to ensure that only complete files are
processed.

**Note:** The files created in the `--output` directory are
  [hardlinks](http://en.wikipedia.org/wiki/Hard_link) pointing at the
  original files in the `--input` directory.
  
#### Splitting input files

The `--split` option will make `wu-sync prepare` split large files in
the `--input` directory into many manageable, smaller files in the
`--output` directory.

Assume that Alice has been busy and that the file
`/data/clean/alice/project_1/file_1` is big.  After running `wu-sync
prepare` twice:

```
$ wu-sync prepare --input=/data/ftp --output=/data/clean --split
$ wu-sync prepare --input=/data/ftp --output=/data/clean --split
```

the `--output` directory `/data/clean` will look like this:

```
/data/clean
├──   alice
│   └──   project_1
│       ├──   file_1.part-0000
│       ├──   file_1.part-0001
│       └──   file_1.part-0002
├──   bob
│   ├──   project_1
│   │   ├──   file_1.part-0000
│   │   ├──   file_2.part-0000
│   │   └──   file_3.part-0000
│   └──   project_2
│       ├──   file_1.part-0000
│       └──   file_2.part-0000
└──   README.part-0000
```

Alice's big `--input` file has been split into three `--output` files.
Bob's `--input` files were smaller than the split size so they each
resulted in a single `--output` file.

You can change the number of lines in each file split with the
`--lines` option and switch to splitting by bytes with the `--bytes`
option.

**Note:** When splitting, the files in the `--output` directory are
  real files instead of hardlinks as they are by default.

#### Ordering output files

Some tools require their input to be ordered and for these tools
`wu-sync prepare` provides an `--ordered` option which will reorganize
files in the `--output` directory so that they are totally ordered:

```
$ wu-sync prepare --input=/data/ftp --output=/data/clean --ordered
$ wu-sync prepare --input=/data/ftp --output=/data/clean --ordered
```

This would result in the following structure for the `--output`
directory:

```
/data/clean
├──   alice
│   └──   2013
│       └──   09
│           └──   20
│               └──   20130920-071142-1-alice-project_1-file_1
├──   bob
│   └──   2013
│       └──   09
│           └──   20
│               ├──   20130920-071142-2-bob-project_1-file_1
│               ├──   20130920-071142-3-bob-project_1-file_2
│               ├──   20130920-071142-4-bob-project_1-file_3
│               ├──   20130920-071142-5-bob-project_2-file_1
│               └──   20130920-071142-6-bob-project_2-file_2
└──   root
    └──   2013
        └──   09
            └──   20
                └──   20130920-071142-0-README
```

The ordering is built up from:

  1. Each top-level subdirectory of the `--input` directory appears in
     the root of the `--output` directory.  (Files that were in the
     `--input` directory itself are put in the `root` subdirectory of
     the `--output` directory.)
	 
  2. Within each top-level subdirectory, a daily directory is created
      based on the time the corresponding `--input` file was
      recognized as **completed**.
	  
  3. All files within the top-level subdirectory of the `--input`
     directory are placed within this daily directory with basenames
     constructed from
	 
	   a. the time the corresponding `--input` file was recognized as completed
	   b. an incrementing counter
	   c. the path of the `--input` file relative to the `--input` directory
	   
The `--ordered` option can be combined with the `--split` option:

```
$ wu-sync prepare --input=/data/ftp --output=/data/clean --ordered
$ wu-sync prepare --input=/data/ftp --output=/data/clean --ordered
```

to get

```
/data/clean
├──   alice
│   └──   2013
│       └──   09
│           └──   20
│               ├──   20130920-071643-1-alice-project_1-file_1.part-0000
│               ├──   20130920-071643-1-alice-project_1-file_1.part-0001
│               └──   20130920-071643-1-alice-project_1-file_1.part-0002
├──   bob
│   └──   2013
│       └──   09
│           └──   20
│               ├──   20130920-071643-2-bob-project_1-file_1.part-0000
│               ├──   20130920-071643-3-bob-project_1-file_2.part-0000
│               ├──   20130920-071643-4-bob-project_1-file_3.part-0000
│               ├──   20130920-071643-5-bob-project_2-file_1.part-0000
│               └──   20130920-071643-6-bob-project_2-file_2.part-0000
└──   root
    └──   2013
        └──   09
            └──   20
                └──   20130920-071643-0-README.part-0000
```
	   
#### Including metadata

The `--metadata` option produces a JSON-formatted metadata file for
each data file produced in the `--output` directory.

Metadata files are stored in a separate hierarchy in the `--output`
directory from the data files.  Within this separate hierarchy they
have the same relative path as their corresponding data files, but
with an extra suffix of `.meta`.  Here's an example:

```
$ wu-sync prepare --input=/data/ftp --output=/data/clean --metadata
$ wu-sync prepare --input=/data/ftp --output=/data/clean --metadata
```

which produces

```
/data/clean
├──   alice
│   └──   2013
│       └──   09
│           └──   20
│               └──   20130920-072539-1-alice-project_1-file_1
├──   alice_meta
│   └──   2013
│       └──   09
│           └──   20
│               └──   20130920-072539-1-alice-project_1-file_1.meta
├──   bob
│   └──   2013
│       └──   09
│           └──   20
│               ├──   20130920-072539-2-bob-project_1-file_1
│               ├──   20130920-072539-3-bob-project_1-file_2
│               ├──   20130920-072539-4-bob-project_1-file_3
│               ├──   20130920-072539-5-bob-project_2-file_1
│               └──   20130920-072539-6-bob-project_2-file_2
├──   bob_meta
│   └──   2013
│       └──   09
│           └──   20
│               ├──   20130920-072539-2-bob-project_1-file_1.meta
│               ├──   20130920-072539-3-bob-project_1-file_2.meta
│               ├──   20130920-072539-4-bob-project_1-file_3.meta
│               ├──   20130920-072539-5-bob-project_2-file_1.meta
│               └──   20130920-072539-6-bob-project_2-file_2.meta
├──   root
│   └──   2013
│       └──   09
│           └──   20
│               └──   20130920-072539-0-README
└──   root_meta
    └──   2013
        └──   09
            └──   20
                └──   20130920-072539-0-README.meta
```

Notice that the `--metadata` option implies the `--ordered` option.
This is so metadata files are guaranteed to come later than their
corresponding data file in any lexicographically ordered walking of
the `--output` directory.

This is so that syncing tools will transfer a metadata file **after**
they transfer its corresponding data file.  Since metadata files are
also small, this means that downstream tools can use the presence or
absence of a metadata file to know for sure whether a data file has
already been transferred completely -- they won't have to do the dance
that `wu-sync prepare` is doing.

The content of a metadata file is very simple.  Here's
`/data/clean/alice_meta/2013/09/20/20130920-072539-1-alice-project_1-file_1.meta`:

```
{
  "path": "alice/2013/09/20/20130920-072539-1-alice-project_1-file_1",
  "meta_path": "alice_meta/2013/09/20/20130920-072539-1-alice-project_1-file_1.meta",
  "size": 168894,
  "md5": "0a61f0919f546ce04fc119b028b88a2e"
}
```

Both `path` and `meta_path` are relative to the `--output` directory.

The `--metadata` option can also be combined with the `--split`
option.

#### Multiple Output Directories

Using multiple `--output` directories mounted on different devices can
greatly speed up operation as large files are read & written.  When
using multiple `--output` directories, each consecutively processed
file in the `--input` directory will be assigned one of the output
directories in a round-robin fashion:

```
$ wu-sync prepare --input=/data/ftp --output=/data/clean_1,/data/clean_2
$ wu-sync prepare --input=/data/ftp --output=/data/clean_1,/data/clean_2
```

### To S3

The `s3` sync will sync data from a local `--input` directory to an
[S3](http://aws.amazon.com/s3/) `--bucket` and path.  It requires the
[`s3cmd`](http://s3tools.org/s3cmd) program.

Here's an example.

```
$ wu-sync s3 --input=/data/clean --bucket=s3://example.com/archive
```

This example assumes that the underlying `s3cmd` has been installed
with appropriate credentials to write to the `s3://example.com` bucket
(or that the bucket `s3://example.com` is world-writable).  If not, an
`s3cmd` configuration file can be passed in with the `--s3cmd_config`
file:

```
$ wu-sync s3 --input=/data/clean --bucket=s3://example.com/archive --s3cmd_config=config/s3cfg
```

The `wu-sync s3` command also works with multiple `--input`
directories.  This is to work nicely with the `wu-sync prepare`
command which has multiple `--output` directories:

```
$ wu-sync s3 --input=/data/clean_1,/data/clean_2 --bucket=s3://example.com/archive --s3cmd_config=config/s3cfg
```

<a name="wu-sync-all">
### Working with multiple data sources

**Note:** This functionality is designed to work in the context of a
  [deploy pack](https://github.com/infochimps-labs/wukong-deploy/tree/leslie)
  only.

Syncing several FTP sources is possibly with `wu-sync` but it's a lot
to type.  The `wu-sync-all` command can read a configuration file with
pre-defined data sources and run each of the sync types on each of the
sources (or just some of them).

Within one of the configuration files in your deploy pack (either
`config/settings.yml` or an environment-specific
`config/environments/ENVIRONMENT.yml`) create a listener for each of
your souces as follows:

```yaml
---
listeners:
  nasa:
    ftp:
      host:     ftp.nasa.gov
      username: narmstrong
      password: first!
      path:     /data/latest
	prepare:
	  ordered:  true
	  metadata: true
    s3:
      bucket: s3://archive.example.com/nasa
  usaf:
    ftp:
      host:     ftp.usaf.gov
      username: bobross
      password: xxx
      path:     /data/latest
	prepare:
	  split: true
	  lines: 1_000_000
    s3:
      bucket: s3://archive.example.com/usaf
  ...
```

The top-level keys in the `listeners` Hash (`nasa`, `usaf`, &c.) are
each the name of a data source.  The next-level keys (`ftp`,
`prepare`, `s3`) each name a sync type and give the options for that
type.  These options are exactly the same as the usual options for
that sync-type.  Options not supplied via the configuration file
(typically "system" parameters like `--input` and `--output`
directories) are expected to be supplied on the command-line at
runtime.

With a proper configuration with valid listeners, the following
commands will perform a sync from FTP to S3 of all data sources:

```
$ wu-sync-all ftp --output=/data/incoming
$ wu-sync-all prepare --input=/data/incoming --output=/data/received
$ wu-sync-all prepare --input=/data/incoming --output=/data/received
$ wu-sync-all s3 --input=/data/received
```

The `--only` and `--except` options can be used to limit
`wu-sync-all`'s action to a desired subset of sources:

```
$ wu-sync-all ftp --output=/data/incoming --only=nasa,usaf
$ wu-sync-all ftp --output=/data/incoming --except=wto
```
