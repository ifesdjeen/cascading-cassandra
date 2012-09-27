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

# Dependency

Jar is hosted on Clojars: https://clojars.org/com.clojurewerkz/cascading-cassandra

## Leiningen

```clojure
[com.clojurewerkz/cascading-cassandra "0.0.3"]
```

## Maven

```xml
<dependency>
  <groupId>com.clojurewerkz</groupId>
  <artifactId>cascading-cassandra</artifactId>
  <version>0.0.3</version>
</dependency>
```
# License

Copyright (C) 2011-2012 Alex Petrov

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# Credits

Alex Petrov: alexp at coffeenco dot de
