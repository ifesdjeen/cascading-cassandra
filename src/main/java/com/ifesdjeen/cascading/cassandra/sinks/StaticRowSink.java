package com.ifesdjeen.cascading.cassandra.sinks;

import cascading.tuple.TupleEntry;
import cascading.tuple.FieldsResolverException;
import com.ifesdjeen.cascading.cassandra.SettingsHelper;
import org.apache.cassandra.thrift.*;

import com.ifesdjeen.cascading.cassandra.hadoop.SerializerHelper;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticRowSink
        implements ISink {

  private static final Logger logger = LoggerFactory.getLogger(StaticRowSink.class);

  public List<Mutation> sink(Map<String, Object> settings,
                             TupleEntry tupleEntry) {

    String rowKeyField = SettingsHelper.getMappingRowKeyField(settings);

    Map<String, String> sinkMappings = SettingsHelper.getSinkMappings(settings);
    int nfields = sinkMappings.size();

    List<Mutation> mutations = new ArrayList<Mutation>(nfields);

    for (String columnName : sinkMappings.keySet()) {
      String columnFieldMapping = sinkMappings.get(columnName);

      if (columnFieldMapping != rowKeyField) {
        Object tupleEntryValue = null;

        try {
          tupleEntryValue = tupleEntry.get(columnFieldMapping);
        } catch (FieldsResolverException e) {
          throw new RuntimeException("Couldn't resolve field: " + columnName);
        }

        if (tupleEntryValue != null) {
          logger.info("Column filed name {}", columnName);
          logger.info("Mapped column name {}", columnFieldMapping);
          logger.info("Column filed value {}", tupleEntryValue);

          Mutation mutation = Util.createColumnPutMutation(SerializerHelper.serialize(columnName),
                  SerializerHelper.serialize(tupleEntryValue));
          mutations.add(mutation);
        }
      }
    }
    return mutations;
  }
}
