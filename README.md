# Wukong-Load

This Wukong plugin makes it easy to load data from the command-line
into various.

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
$ sudo gem install wukong-hadoop
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

### Elasticsearch Usage

Lets you load JSON-formatted records into an
[Elasticsearch](http://www.elasticsearch.org) database.  See full
options with

```
$ wu-load elasticsearch --help
```

#### Expected Input

All input to `wu-load` should be newline-separated, JSON-formatted,
hash-like record.  Some keys in the record will be interpreted as
metadata about the record or about how to route the record within the
database but the entire record will be written to the database
unmodified.

A (pretty-printed for clarity -- the real record shouldn't contain
newlines) record like

```json
{
  "_index":      "publications"
  "_type":       "book",
  "ISBN":        "0553573403",
  "title":       "A Game of Thrones",
  "author":      "George R. R. Martin",
  "description": "The first of half a hundred novels to come out since...",
  ...
}
```

might use the `_index` and `_type` fields as metadata but the
**whole** record will be written.

#### Connecting

`wu-load` has a default host (localhost) and port (9200) it tries to
connect to but you can change these:

```
$ cat data.json | wu-load elasticsearch --host=10.122.123.124 --port=80
```

All queries will be sent to this address.

#### Routing

Elasticsearch stores data in several *indices* which each contain
*documents* of various *types*.

`wu-load` loads each document into default index (`wukong`) and type
(`streaming_record`), but you can change these:

```
$ cat data.json | wu-load elasticsearch --host=10.123.123.123 --index=publication --es_type=book
```

##### Creates vs. Updates

If an input document contains a value for the field `_id` then that
value will be as the ID of the record when written, possibly
overwriting a record that already exists -- an update.

You can change the field you use for the Elasticsearch ID property:

```
$ cat data.json | wu-load elasticsearch --host=10.123.123.123 --index=media --es_type=books --id_field="ISBN"
```
