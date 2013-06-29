package com.ifesdjeen.cascading.cassandra;

import java.util.*;
import java.nio.ByteBuffer;
import java.io.IOException;

import cascading.tuple.Tuple;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.db.IColumn;


public interface ISource {

  void source(Map<String, Object> settings,
              SortedMap<ByteBuffer, IColumn> value,
              Tuple result) throws IOException;
}
