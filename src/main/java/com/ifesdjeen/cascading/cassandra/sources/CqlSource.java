package com.ifesdjeen.cascading.cassandra.sources;

import org.apache.cassandra.db.marshal.AbstractType;
import cascading.scheme.SourceCall;
import cascading.tuple.Tuple;
import com.ifesdjeen.cascading.cassandra.SettingsHelper;
import com.ifesdjeen.cascading.cassandra.hadoop.SerializerHelper;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

public class CqlSource implements Serializable,IConfigurableSource {

  private Map<String,Object> settings;

  @Override
  public void configure(Map<String, Object> settings) {
    this.settings = settings;
  }

  @Override
  public void sourcePrepare(SourceCall<Object[], RecordReader> sourceCall) {
    Map<String,ByteBuffer> key = (Map<String, ByteBuffer>) sourceCall.getInput().createKey();
    Map<String,ByteBuffer> value = (Map<String, ByteBuffer>) sourceCall.getInput().createValue();

    Object[] obj = new Object[]{key, value};

    sourceCall.setContext(obj);
  }

  @Override
  public Tuple source(Object boxedKey, Object boxedValue) throws IOException {
    Map<String, String> dataTypes = SettingsHelper.getTypes(settings);

    Tuple result = new Tuple();
    Map<String,ByteBuffer> keys = (Map<String,ByteBuffer>) boxedKey;
    Map<String,ByteBuffer> columns = (Map<String,ByteBuffer>) boxedValue;

    for(Map.Entry<String, ByteBuffer> key : keys.entrySet()) {
      try {
        result.add(SerializerHelper.deserialize(key.getValue(),
                SerializerHelper.inferType(dataTypes.get(key.getKey()))));
      } catch (Exception e) {
        throw new RuntimeException("Couldn't deserialize key: " + key.getKey(), e);
      }
    }

    for(Map.Entry<String, ByteBuffer> column : columns.entrySet()) {
      try {
        ByteBuffer serializedVal = column.getValue();
        Object val = null;
        if (serializedVal != null) {
          AbstractType type = SerializerHelper.inferType(dataTypes.get(column.getKey()));
          val = SerializerHelper.deserialize(serializedVal, type);
        }
        result.add(val);
      } catch (Exception e) {
        throw new RuntimeException("Couldn't deserialize value for: " + column.getKey(), e);
      }
    }

    return result;
  }
}
