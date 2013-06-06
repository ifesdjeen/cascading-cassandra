package com.ifesdjeen.cascading.cassandra;

import cascading.tuple.TupleEntry;
import org.apache.cassandra.thrift.*;
import java.util.*;

import java.nio.ByteBuffer;


public interface ISink {
    List<Mutation> sink( Map<String, Object> settings,
                         TupleEntry tupleEntry );

    public static class Util {

        public static Mutation createColumnPutMutation(ByteBuffer name, ByteBuffer value) {
            Column column = new Column(name);
            column.setName(name);
            column.setValue(value);
            column.setTimestamp(System.currentTimeMillis());

            Mutation m = new Mutation();
            ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
            columnOrSuperColumn.setColumn(column);
            m.setColumn_or_supercolumn(columnOrSuperColumn);

            return m;
        }
    }
}
