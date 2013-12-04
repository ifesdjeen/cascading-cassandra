package com.ifesdjeen.cascading.cassandra.sources;

import cascading.scheme.SourceCall;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.mapred.RecordReader;

import java.nio.ByteBuffer;
import java.util.SortedMap;

public abstract class BaseThriftSource implements ISource {

  public void sourcePrepare(SourceCall<Object[], RecordReader> sourceCall) {
    ByteBuffer key = ByteBufferUtil.clone((ByteBuffer) sourceCall.getInput().createKey());
    SortedMap<ByteBuffer, Column> value = (SortedMap<ByteBuffer, Column>) sourceCall.getInput().createValue();

    Object[] obj = new Object[]{key, value};
    sourceCall.setContext(obj);
  }
}