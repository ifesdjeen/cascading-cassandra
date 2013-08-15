package com.ifesdjeen.cascading.cassandra.sinks;

import cascading.tuple.TupleEntry;
import com.ifesdjeen.cascading.cassandra.SettingsHelper;
import com.ifesdjeen.cascading.cassandra.hadoop.SerializerHelper;
import org.apache.hadoop.mapred.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CqlSink implements Serializable,IConfigurableSink {

  private static final Logger logger = LoggerFactory.getLogger(CqlSink.class);

  private Map<String,String> cqlKeyMappings;
  private Map<String,String> cqlValueMappings;

  @Override
  public void configure(Map<String, Object> settings) {
    this.cqlKeyMappings = SettingsHelper.getCqlKeyMappings(settings);
    this.cqlValueMappings = SettingsHelper.getCqlValueMappings(settings);
  }

  public void sink(TupleEntry tupleEntry, OutputCollector outputCollector)
          throws IOException {
    Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
    List<ByteBuffer> values = new ArrayList<ByteBuffer>();

    for (String key: cqlKeyMappings.keySet()) {
      keys.put(key, SerializerHelper.serialize(tupleEntry.getObject(cqlKeyMappings.get(key))));
    }

    for (String value: cqlValueMappings.keySet()) {
      values.add(SerializerHelper.serialize(tupleEntry.getObject(cqlValueMappings.get(value))));
    }

    outputCollector.collect(keys, values);
  }
}