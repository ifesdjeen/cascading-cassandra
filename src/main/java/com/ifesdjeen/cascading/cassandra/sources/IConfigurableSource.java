package com.ifesdjeen.cascading.cassandra.sources;

import java.util.*;
import java.io.IOException;

import cascading.scheme.SourceCall;
import cascading.tuple.Tuple;
import org.apache.hadoop.mapred.RecordReader;


/**
 * ISource is used to allow flexibility when dealing with different input sources
 * from Cassandra, and deal with cases such as Dynamic/Static columns, custom
 * serialization etc.
 */
public interface IConfigurableSource {

  /**
   * configure the source : called once afer source construction
   *
   * @param settings - settings object passed while constructing CassandraScheme
   *
   */
  public void configure(Map<String, Object> settings);


  /**
   * Creates initial (empty) tuple
   *
   * @param sourceCall
   * @return
   */
  public void sourcePrepare(SourceCall<Object[], RecordReader> sourceCall);

  /**
   * Convert `value` map (key/value pairs) to Cascading tuple.
   *
   * @param value - key/value pairs of column names and columns (values)
   * @param key - row (partition) key
   * @return
   * @throws IOException
   */
  public Tuple source(Object key,
                      Object value) throws IOException;
}
