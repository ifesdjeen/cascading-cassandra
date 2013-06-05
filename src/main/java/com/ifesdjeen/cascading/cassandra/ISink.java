package com.ifesdjeen.cascading.cassandra;

import cascading.tuple.TupleEntry;
import org.apache.cassandra.thrift.*;
import java.util.*;

public interface ISink {
    List<Mutation> sink( Map<String, Object> settings,
                         TupleEntry tupleEntry );
}
