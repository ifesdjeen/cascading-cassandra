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


# Usage

To use it as both source and sink, simply create a Schema:

```java
import com.clojurewerkz.cascading.cassandra.CassandraTap;
import com.clojurewerkz.cascading.cassandra.CassandraScheme;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap

Map<String, String> settings = new HashMap<String, String>();
mappings.put("db.host", "localhost");
mappings.put("db.port", "9160");
// And so on...

CassandraScheme scheme = new CassandraScheme(settings);
CassandraTap tap = new CassandraTap(scheme);
```

That's pretty much it. To do same thing in Clojure (with Cascalog),
you can use following code:

```clojure

(defn create-tap
  []
  (let [keyspace      "keyspace"
        column-family "column-family"
        scheme        (CassandraScheme.
                       {"sink.keyColumnName" "name"
                        "db.host" "127.0.0.1"
                        "db.port" "9160"
                        "db.keyspace" "cascading_cassandra"
                        "db.inputPartitioner" "org.apache.cassandra.dht.Murmur3Partitioner"
                        "db.outputPartitioner" "org.apache.cassandra.dht.Murmur3Partitioner"})
        tap           (CassandraTap. scheme)]
    tap))
```

# Possible mappings:

DB:
  * `db.host` - host of the database to connect to
  * `db.port` - port of the database to connect to
  * `db.keyspace` - keyspace to use for sink or source
  * `db.columnFamily` - column family  to use for sink or source
  * `db.inputPartitioner` - partitiner for DB used as source
  * `db.outputPartitioner` - partitiner for DB used as sink

Source:
  * `source.columns` - columns for the source, to be fetched
  * `source.useWideRows` - wether or not to use wide rows deserialization
  * `source.types` - data types to use for deserialization.
    * Examplpe for wide columns: `{"key", "UTF8Type", "value" "Int32Type"}`
    * Example for static columns: `{"column1" "UTF8Type" "column2" "Int32Type"`

Sink:
  * `sink.keyColumnName` - key column name for sink
  * `sink.outputMappings` - output mappings for sink, used to map internal Cascading
    tuple segment names to database columns.

# Dependency

Jar is hosted on Clojars: https://clojars.org/cascading-cassandra

## Leiningen

```clojure
[cascading-cassandra "1.0.0-rc3"]
```

## Maven

```xml
<dependency>
  <groupId>com.clojurewerkz</groupId>
  <artifactId>cascading-cassandra</artifactId>
  <version>1.0.0-rc3</version>
</dependency>
```

# This project supports the ClojureWerkz goals

[ClojureWerkz](http://clojurewerkz.org/) is a growing collection of open-source, **batteries-included Clojure libraries** that emphasise modern targets, great documentation, and thorough testing. They've got a ton of great stuff, check 'em out!

# License

Copyright (C) 2011-2013 Alex Petrov

Double licensed under the Eclipse Public License (the same as Clojure) or
the Apache Public License 2.0.

[Other Contributors](github.com/ifesdjeen/cascading-cassandra)

# Thanks

Thanks to (JetBrains)[http://www.jetbrains.com/] for providing a license
for (IntelliJIDEA)[http://www.jetbrains.com/idea/] to develop part
of this project developed in Java.

# Credits

[Alex Petrov](https://twitter.com/ifesdjeen): alexp at coffeenco dot de
