# Cascading Tap for Cassandra

This is a Cassandra Tap that can be used as a sink and source. Works
with the latest version of Cassandra and Cascading (2.0), is tested,
well-maintained. It's working fine for us, but use it at your own
risk.

# Usage

To use it as both source and sink, simply create a Schema:

```java
import com.clojurewerkz.cascading.cassandra.CassandraTap;
import com.clojurewerkz.cascading.cassandra.CassandraScheme;

import java.util.ArrayList;

// List of columns to be fetched from Cassandra
ArrayList<String> columns = new ArrayList<String>();
columns.add("first-column-name");
columns.add("second-column-name");
columns.add("third-column-name");

// When writing back to cassandra, you may have Cascading output tuple item names
// a bit different from your Cassandra ColumnFamily definition. Otherwise, you can
// simply specify both key and value same.
HashMap<String, String> mappings = new HashMap<String, String>();
mappings.put("first-column-name", "cascading-output-tuple-first-item-name");
mappings.put("second-column-name", "cascading-output-tuple-second-item-name");
mappings.put("third-column-name", "cascading-output-tuple-third-item-name");

CassandraScheme scheme = new CassandraScheme("127.0.0.1"
                                             "9160"
                                             "keyspace-name"
                                             "column-family-name"
                                             "key-column-name"
                                             columns
                                             mappings);

CassandraTap tap = new CassandraTap(scheme);
```

That's pretty much it. To do same thing in Clojure (with Cascalog),
you can use following code:

```clojure

(defn create-tap
  []
  (let [keyspace      "keyspace"
        column-family "column-family"
        scheme        (CassandraScheme. "127.0.0.1"
                                        "9160"
                                        keyspace
                                        column-family
                                        "key-column-name"
                                        (java.util.ArrayList. ["first-column-name" "second-column-name" "third-column-name"])
                                        (java.util.HashMap. {"first-column-name" "cascading-output-tuple-first-item-name"
                                                             "second-column-name" "cascading-output-tuple-second-item-name"
                                                             "third-column-name" "cascading-output-tuple-third-item-name"}))
        tap           (CassandraTap. scheme)]
    tap))
```

# License

Copyright (C) 2011-2012 Alex Petrov

Distributed under the Eclipse Public License, the same as Clojure.

