package com.ifesdjeen.cascading.cassandra.sinks;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.FieldsResolverException;
import com.ifesdjeen.cascading.cassandra.SettingsHelper;
import org.apache.cassandra.thrift.*;

import com.ifesdjeen.cascading.cassandra.hadoop.SerializerHelper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.hadoop.mapred.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticRowSink implements ISink {

  private static final Logger logger = LoggerFactory.getLogger(StaticRowSink.class);

  public void sink(Map<String, Object> settings, TupleEntry tupleEntry, OutputCollector outputCollector)
          throws IOException {

    String rowKeyField = SettingsHelper.getMappingRowKeyField(settings);

    Tuple key = tupleEntry.selectTuple(new Fields(rowKeyField));
    ByteBuffer keyBuffer = SerializerHelper.serialize(key.get(0));

    Map<String, String> sinkMappings = SettingsHelper.getSinkMappings(settings);
    int nfields = sinkMappings.size();

    List<Mutation> mutations = new ArrayList<Mutation>(nfields);

    for (String columnName : sinkMappings.keySet()) {
      String columnFieldMapping = sinkMappings.get(columnName);

      if (columnFieldMapping != rowKeyField) {
        Object tupleEntryValue;

        try {
          tupleEntryValue = tupleEntry.get(columnFieldMapping);
        } catch (FieldsResolverException e) {
          throw new RuntimeException("Couldn't resolve field: " + columnName);
        }

        if (tupleEntryValue != null) {
          logger.debug("Column filed name {}", columnName);
          logger.debug("Mapped column name {}", columnFieldMapping);
          logger.debug("Column filed value {}", tupleEntryValue);

          Mutation mutation = Util.createColumnPutMutation(SerializerHelper.serialize(columnName),
                  SerializerHelper.serialize(tupleEntryValue));
          mutations.add(mutation);
        }
      }
    }

    outputCollector.collect(keyBuffer, mutations);
  }
}
