package com.ifesdjeen.cascading.cassandra;

import java.util.*;
import java.nio.ByteBuffer;
import java.io.IOException;

import cascading.tuple.Tuple;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.db.IColumn;


public interface ISource {

    void source( Map<String, Object> settings,
                 SortedMap<ByteBuffer, IColumn> value,
                 Tuple result ) throws IOException;

    public static class Util {

        public static  Map<String, String> getSourceTypes( Map<String, Object> settings ) {
            if (settings.containsKey("source.types")) {
                return (Map<String, String>) settings.get("source.types");
            } else {
                return new HashMap<String, String>();
            }
        }
    }

}
