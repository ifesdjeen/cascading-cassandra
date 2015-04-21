# PROJECT IS SEARCHING FOR MAINTAINERS!

Releases so far have been used in production in multiple companies. However, 
at the moment Alex (@ifesdjeen) doesn't use Cascading in production
anymore. Therefore, keeping project up to date is hard and is nearly impossible, 
since there's a lot of quircks and small things that go wrong with every
release of both Hadoop and Cassandra.

# Cascading Tap for Cassandra

[![Build Status](https://secure.travis-ci.org/ifesdjeen/cascading-cassandra.png)](http://travis-ci.org/ifesdjeen/cascading-cassandra)

This is a Cassandra Tap that can be used as a sink and source. Works
with the latest version of Cassandra and Cascading (2.0), is tested,
well-maintained. It's working fine for us, but use it at your own
risk.

If you're new to Cassandra, check out our [Cassandra Guides](http://clojurecassandra.info/articles/guides.html),
They were initially written for [Cassaforte](https://github.com/clojurewerkz/cassaforte), Clojure Cassandra
driver, but are generic enough and go in elaborate details on all Cassandra-related topics, such as
[Consistency/Availability](http://clojurecassandra.info/articles/cassandra_concepts.html), [Data Modelling](http://clojurecassandra.info/articles/data_modelling.html),
[Command Line Tools](http://clojurecassandra.info/articles/troubleshooting.html#toc_2), [Timestamps](http://clojurecassandra.info/articles/kv.html#toc_3),
[Counters](http://clojurecassandra.info/articles/kv.html#toc_5) and many many more.


## Usage

To use it as both source and sink, simply create a Schema:

```java
import com.clojurewerkz.cascading.cassandra.CassandraTap;
import com.clojurewerkz.cascading.cassandra.cql3.CassandraCQL3Scheme;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap

Map<String, String> settings = new HashMap<String, String>();
mappings.put("db.host", "localhost");
mappings.put("db.port", "9160");
// And so on...

CassandraCQL3Scheme scheme = new CassandraCQL3Scheme(settings);
CassandraTap tap = new CassandraTap(scheme);
```

That's pretty much it. To do same thing in Clojure (with Cascalog),
you can use following code:

```clojure

(defn create-tap
  []
  (let [scheme        (CassandraCQL3Scheme.
                       {"db.host" "127.0.0.1"
                        "db.port" "9160"
                        "db.keyspace" "cascading_cassandra"
                        "db.inputPartitioner" "org.apache.cassandra.dht.Murmur3Partitioner"
                        "db.outputPartitioner" "org.apache.cassandra.dht.Murmur3Partitioner"
                        "db.columnFamily" "column_family"
                        "mappings.cqlKeys" ["id" "version:?ver"]
                        "mappings.cqlValues" ["date" "count:?count"]
                        "types" {"id"      "UTF8Type"
                                 "version" "Int32Type"
                                 "date"    "DateType"
                                 "count"   "DecimalType"}})
        tap           (CassandraTap. scheme)]
    tap))
```

## Possible mappings:

### DB:
  * `db.host` - host of the database to connect to
  * `db.port` - port of the database to connect to
  * `db.keyspace` - keyspace to use for sink or source
  * `db.inputPartitioner` - partitiner for DB used as source
  * `db.outputPartitioner` - partitiner for DB used as sink
  * `db.columnFamily` - column family  to use for sink or source

If you're not familiar with Static / Dynamic terminology, please refer to [this guide](http://clojurecassandra.info/articles/data_modelling.html)

### Static Row Source:

  * `types` - hashmap specifying type (e.q. `UTF8Type` of each column)
  * `mappings.source` - a `List<String>` (list of strings) specifying, in an order items are appearing
    in Cascading output, mappings of values to corresponding Cassandra fields

### Static Row Sink:

  * `types` - hashmap specifying type (e.q. `UTF8Type` of each column)
  * `mappings.rowKey` - row key (name of your partition key from Cassandra), so that Tap would know
    which key to use as a partition key

### Dynamic Row Source

  * `types.dynamic` - hashmap specifying mapping of `rowKey` to it's type, `columnName` to it's type
    and `columnValue` to it's type
  * `mappings.dynamic` - hashmap specifying mapping of `rowKey`, `columnName` and `columnValue` to
    the fields that correspond to them in Cascading output

### Dynamic Row Sink:

  * `types.dynamic` - hashmap specifying mapping of `rowKey` to it's type, `columnName` to it's type
    and `columnValue` to it's type
  * `mappings.dynamic` - hashmap specifying mapping of `rowKey`, `columnName` and `columnValue` to
    the fields that correspond to them in Cascading output

Please note that only given configuration parameters are valid for usage, you can not create a single
universal dynamic/static Source/Sink.

If cascalog vars are omitted from mappings, they default to `?<c*-col-name>` for primary-key columns, and to `!<c*-col-name>` for value columns

## Using Static (narrow) tables

If you're working with datasets that have a non-compound key, it's called "Static Table".
For example, you have a table that stores user data, where users are accessed by unique name
identifier.

Let's say you have a table called "Libraries":

```sql
CREATE TABLE users (name varchar,
                    language varchar,
                    schmotes int,
                    votes int,
                    PRIMARY KEY (name))
WITH COMPACT STORAGE;
```

__Please note that it is very important to add WITH COMPACT STORAGE__ to your table creation.

Typically, data in such table looks like:

```
| :name               | :language |   :votes |
|---------------------+-----------+----------|
| Cassaforte          | Clojure   |       10 |
| Cascading Cassandra | Java      |       20 |
| Langohr             | Clojure   |       12 |
```

### Using Static Row Source Tap

In order to query data from it, you should:

  * specify `columnFamily`
  * specify types mappings
  * specify order for non-key (data) columns:

```java
Map<String, Object> config = new HashMap<>();
// Default Settings
config.put("db.host", "127.0.0.1");
config.put("db.port", "19160");
config.put("db.keyspace", "cascading_cassandra");
config.put("db.inputPartitioner", "org.apache.cassandra.dht.Murmur3Partitioner");
config.put("db.outputPartitioner", "org.apache.cassandra.dht.Murmur3Partitioner");

//Example-specific settings
config.put("db.columnFamily", "libraries");

// Put mappings of types, specifying which source field has which type
Map<String, String> types = new HashMap<>();
types.put("name",      "UTF8Type");
types.put("language",  "UTF8Type");
types.put("schmotes",  "Int32Type");
types.put("votes",     "Int32Type");
config.put("types", types);

// Configure input columns in an order they should appear in client code
config.put("mappings.source", Arrays.asList("language", "schmotes", "votes"));

CassandraScheme scheme = new CassandraScheme(config);
CassandraTap tap = new CassandraTap(scheme);
```

### Using Static Row Sink Tap

In order to sink items into the Cascading, you should specify

  * type mappings
  * name of the row key
  * mappings of columns for sinking

```java
Map<String, Object> config = new HashMap<>();
// Default Settings
config.put("db.host", "127.0.0.1");
config.put("db.port", "19160");
config.put("db.keyspace", "cascading_cassandra");
config.put("db.inputPartitioner", "org.apache.cassandra.dht.Murmur3Partitioner");
config.put("db.outputPartitioner", "org.apache.cassandra.dht.Murmur3Partitioner");

//Example-specific settings
config.put("db.columnFamily", "libraries_wide");

// Put mappings of types, specifying which source field has which type
Map<String, String> types = new HashMap<>();
types.put("name",      "UTF8Type");
types.put("language",  "UTF8Type");
types.put("schmotes",  "UTF8Type");
types.put("votes",     "Int32Type");

config.put("types", types);

// Specify a row key, so that Tap would know which key to use as a partition key
config.put("mappings.rowKey", "name");

// Specify sink column mappings, this is required to map your Cascalog fields (right ones) to
// Internal Cassandra fields (left)
Map<String, String> mappings = new HashMap<>();
mappings.put("name",      "?value1");
mappings.put("language",  "?value2");
mappings.put("schmotes",  "?value3");
mappings.put("votes",     "?value4");

config.put("mappings.sink", mappings);

CassandraScheme scheme = new CassandraScheme(config);
CassandraTap tap = new CassandraTap(scheme);
```

## Using Dynamic (wide) Rows

In current Cassandra terminology, term Compound Key is used to describe entries that
are identified by the set of keys. This terms is used to avoid ambiguity with
Composite Columns that were used in previous versions of Cassandra.

Queries with locked partition key are not expensive, since you can guarantee that
things that have same partition key will be located on the same node.

```sql
CREATE TABLE libraries_wide (name varchar
                             language :varchar
                             votes int
                             PRIMARY KEY (name, language))
WITH COMPACT STORAGE;
```

### Using Wide Rows

In order to use Dynamic wide row sources, you should use different mappings (due
to the fact that an internal implementation is entirely different from static row
sources.

### Using Wide Row Source Tap

For that, you'll have to specify

  * row key type
  * column name type
  * column value type
  * mappings from cascading triplet names to row key, column name and column value in Cassandra

```java
Map<String, Object> config = new HashMap<>();
// Default Settings
config.put("db.host", "127.0.0.1");
config.put("db.port", "19160");
config.put("db.keyspace", "cascading_cassandra");
config.put("db.inputPartitioner", "org.apache.cassandra.dht.Murmur3Partitioner");
config.put("db.outputPartitioner", "org.apache.cassandra.dht.Murmur3Partitioner");

//Example-specific settings
config.put("db.columnFamily", "libraries_wide");

// Put mappings of types, specifying which source field has which type
Map<String, String> types = new HashMap<>();
types.put("rowKey",      "UTF8Type");
types.put("columnName",  "UTF8Type");
types.put("columnValue", "Int32Type");

config.put("types.dynamic", types);

// Specify sink column mappings, this is required to map your Cascalog fields (right ones) to
// Internal Cassandra fields (left)
Map<String, String> mappings = new HashMap<>();
mappings.put("rowKey",      "?value1");
mappings.put("columnName",  "?value2");
mappings.put("columnValue", "?value3");

config.put("mappings.dynamic", mappings);

CassandraScheme scheme = new CassandraScheme(config);
CassandraTap tap = new CassandraTap(scheme);
```

### Using Wide Row Sink Tap

For that, you'll have to specify

  * a row key (partition key) name and type
  * a column name (whatever is going to play a role of column name in dynamic table) type
  * a value type

```java
Map<String, Object> config = new HashMap<>();
// Default Settings
config.put("db.host", "127.0.0.1");
config.put("db.port", "19160");
config.put("db.keyspace", "cascading_cassandra");
config.put("db.inputPartitioner", "org.apache.cassandra.dht.Murmur3Partitioner");
config.put("db.outputPartitioner", "org.apache.cassandra.dht.Murmur3Partitioner");

//Example-specific settings
config.put("db.columnFamily", "libraries_wide");

// Put mappings of types, specifying which source field has which type
Map<String, String> types = new HashMap<>();
types.put("rowKey",      "UTF8Type"); // This is your primary (partition) key, first key in sequence
types.put("columnName",  "DateType"); // This is your column name (second part of the key, if the key's not compound)
types.put("columnValue", "UTF8Type"); // This is the type of your value
// You will get a triplet of <row key>, <column name>, <column value> in return from Cascading

config.put("types.dynamic", types);

// Specify sink column mappings, this is required to map your Cascalog fields (right ones) to
// Internal Cassandra fields (left)
Map<String, String> mappings = new HashMap<>();
mappings.put("rowKey",      "?value1");
mappings.put("columnName",  "?value2");
mappings.put("columnValue", "?value3");

config.put("mappings.dynamic", mappings);

CassandraScheme scheme = new CassandraScheme(config);
CassandraTap tap = new CassandraTap(scheme);
```

## Dependency Information

Jar is hosted on Clojars: https://clojars.org/cascading-cassandra

### Leiningen

```clojure
[cascading-cassandra "2.0.6"]
```

### Maven

```xml
<dependency>
  <groupId>cascading-cassandra</groupId>
  <artifactId>cascading-cassandra</artifactId>
  <version>2.0.6</version>
</dependency>
```

## This project supports the ClojureWerkz goals

[ClojureWerkz](http://clojurewerkz.org/) is a growing collection of open-source, **batteries-included Clojure libraries** that emphasise modern targets, great documentation, and thorough testing. They've got a ton of great stuff, check 'em out!

## Typical Problems

If you see that in your stacktrace: 

```
Caused by: InvalidRequestException(why:Not enough bytes to read value of component 0)
        at org.apache.cassandra.thrift.Cassandra$batch_mutate_result.read(Cassandra.java:20833)
        at org.apache.thrift.TServiceClient.receiveBase(TServiceClient.java:78)
```

You've forgotten to addd "WITH COMPACT STORAGE" to your table creation script.


## License

Copyright (C) 2011-2013 Alex Petrov, Craig McMillan

Double licensed under the Eclipse Public License (the same as Clojure) or
the Apache Public License 2.0.

[Other Contributors](github.com/ifesdjeen/cascading-cassandra/contributors)

## Thanks

  * Thanks to [JetBrains](http://www.jetbrains.com/) for providing a license
    for [IntelliJIDEA](http://www.jetbrains.com/idea/) to develop part
    of this project developed in Java.

  * Thanks to YourKit for supporting cascading-cassandra with licenses for its full-featured Java Profiler.
    YourKit, LLC is the creator of innovative and intelligent tools for profiling
    Java and .NET applications. Take a look at YourKit's leading software products:
      * [YourKit Java Profiler](http://www.yourkit.com/java/profiler/index.jsp) and
      * [YourKit .NET Profiler](http://www.yourkit.com/.net/profiler/index.jsp).


[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/ifesdjeen/cascading-cassandra/trend.png)](https://bitdeli.com/free "Bitdeli Badge")

