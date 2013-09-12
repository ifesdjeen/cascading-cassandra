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

# Possible mappings:

### DB:
  * `db.host` - host of the database to connect to
  * `db.port` - port of the database to connect to
  * `db.keyspace` - keyspace to use for sink or source
  * `db.inputPartitioner` - partitiner for DB used as source
  * `db.outputPartitioner` - partitiner for DB used as sink
  * `db.columnFamily` - column family  to use for sink or source

### Source:
  * `mappings.cqlKeys`   - primary-key columns
  * `mappings.cqlValues` - value columns
  * `types` - map of c* column names to c* column types to use for deserialization.

Sourced tuples will contain the primary-key columns followed by the value columns

### Sink:
  * `mappings.cqlKeys`   - primary key column mappings in form `<c*-col-name>[:<cascalog-var>]`
  * `mappings.cqlValues` - value column mappings in form `<c*-col-name>[:<cascalog-var>]`

If cascalog vars are omitted from mappings, they default to `?<c*-col-name>` for primary-key columns, and to `!<c*-col-name>` for value columns

# Dependency

Jar is hosted on Clojars: https://clojars.org/cascading-cassandra

## Leiningen

```clojure
[cascading-cassandra "1.0.0-rc5"]
```

## Maven

```xml
<dependency>
  <groupId>com.clojurewerkz</groupId>
  <artifactId>cascading-cassandra</artifactId>
  <version>1.0.0-rc5</version>
</dependency>
```

# This project supports the ClojureWerkz goals

[ClojureWerkz](http://clojurewerkz.org/) is a growing collection of open-source, **batteries-included Clojure libraries** that emphasise modern targets, great documentation, and thorough testing. They've got a ton of great stuff, check 'em out!

# License

Copyright (C) 2011-2013 Alex Petrov, Craig McMillan

Double licensed under the Eclipse Public License (the same as Clojure) or
the Apache Public License 2.0.

[Other Contributors](github.com/ifesdjeen/cascading-cassandra/contributors)

# Thanks

* Thanks to (JetBrains)[http://www.jetbrains.com/] for providing a license
  for (IntelliJIDEA)[http://www.jetbrains.com/idea/] to develop part
  of this project developed in Java.

* Thanks to YourKit for supporting cascading-cassandra with licenses for its full-featured Java Profiler.
  YourKit, LLC is the creator of innovative and intelligent tools for profiling
  Java and .NET applications. Take a look at YourKit's leading software products:
    * (YourKit Java Profiler)[http://www.yourkit.com/java/profiler/index.jsp] and
    * (YourKit .NET Profiler)[http://www.yourkit.com/.net/profiler/index.jsp].

# Credits

[Alex Petrov](https://twitter.com/ifesdjeen): alexp at coffeenco dot de
