package com.ifesdjeen.cascading.cassandra.sources;

import java.util.*;
import java.nio.ByteBuffer;
import java.io.IOException;

import com.ifesdjeen.cascading.cassandra.SettingsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.tuple.Tuple;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.ifesdjeen.cascading.cassandra.hadoop.SerializerHelper;


/**
 * StaticRowSource is intended for usage with static sources, as the name suggests.
 */
public class StaticRowSource extends BaseThriftSource implements ISource {

  private static final Logger logger = LoggerFactory.getLogger(StaticRowSource.class);

  public Tuple source(Map<String, Object> settings,
                      Object boxedKey,
                      Object boxedColumns) throws IOException {
    SortedMap<ByteBuffer, Column> columns = (SortedMap<ByteBuffer, Column>) boxedColumns;
    ByteBuffer key = (ByteBuffer) boxedKey;

    Tuple result = new Tuple();
    result.add(ByteBufferUtil.string(key));

    Map<String, String> dataTypes = SettingsHelper.getTypes(settings);
    List<String> sourceMappings = SettingsHelper.getSourceMappings(settings);

    Map<String, Column> columnsByStringName = new HashMap<String, Column>();
    for (ByteBuffer columnName : columns.keySet()) {
      String stringName = ByteBufferUtil.string(columnName);
      logger.debug("column name: {}", stringName);
      Column col = columns.get(columnName);
      logger.debug("column: {}", col);
      columnsByStringName.put(stringName, col);
    }

    for (String columnName : sourceMappings) {
      AbstractType columnValueType = SerializerHelper.inferType(dataTypes.get(columnName));
      if (columnValueType != null) {
        try {
          Column column = columnsByStringName.get(columnName);
          ByteBuffer serializedVal = column.value();
          Object val = null;
          if (serializedVal != null) {
            val = SerializerHelper.deserialize(serializedVal, columnValueType);
          }
          logger.debug("Putting deserialized column: {}. {}", columnName, val);
          result.add(val);
        } catch (Exception e) {
          throw new RuntimeException("Couldn't deserialize column: " + columnName, e);
        }
      } else {
        throw new RuntimeException("no type given for column: " + columnName);
      }
    }

    return result;
  }
}
