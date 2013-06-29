package com.ifesdjeen.cascading.cassandra;

import java.util.*;
import java.nio.ByteBuffer;
import java.io.IOException;

import cascading.tuple.Tuple;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.db.IColumn;


/**
 * ISource is used to allow flexibility when dealing with different input sources
 * from Cassandra, and deal with cases such as Dynamic/Static columns, custom
 * serialization etc.
 */
public interface ISource {


  /**
   * Convert `value` map (key/value pairs) to Cascading tuple.
   *
   * @param settings - settings object passed while constructing CassandraScheme
   * @param value - key/value pairs of column names and columns (values)
   * @param key - row (partition) key
   * @return
   * @throws IOException
   */
  public Tuple source(Map<String, Object> settings,
                      SortedMap<ByteBuffer, IColumn> value,
                      ByteBuffer key) throws IOException;
}
