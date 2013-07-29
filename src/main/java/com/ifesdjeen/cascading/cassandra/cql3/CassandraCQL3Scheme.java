package com.ifesdjeen.cascading.cassandra.cql3;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import com.ifesdjeen.cascading.cassandra.BaseCassandraScheme;
import com.ifesdjeen.cascading.cassandra.SettingsHelper;
import com.ifesdjeen.cascading.cassandra.hadoop.SerializerHelper;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.SortedMap;

public class CassandraCQL3Scheme extends BaseCassandraScheme {

  public CassandraCQL3Scheme(Map<String, Object> settings) {
    super(settings);
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> process,
                             Tap<JobConf, RecordReader, OutputCollector> tap,
                             JobConf conf) {
    super.sourceConfInit(process, tap, conf);

    ConfigHelper.setInputColumnFamily(conf, this.keyspace, this.columnFamily);
    conf.setInputFormat(CqlPagingInputFormat.class);

    if (this.settings.containsKey("source.CQLPageRowSize")) {
      CqlConfigHelper.setInputCQLPageRowSize(conf, (String) this.settings.get("source.CQLPageRowSize"));
    } else {
      CqlConfigHelper.setInputCQLPageRowSize(conf, "100");
    }

    if (this.settings.containsKey("source.whereClauses")) {
      CqlConfigHelper.setInputWhereClauses(conf, (String) this.settings.get("source.whereClauses"));
    }

    conf.set("row_key", "name");
  }

  /**
   *
   * @param flowProcess
   * @param sourceCall
   */
  @Override
  public void sourcePrepare(FlowProcess<JobConf> flowProcess,
                            SourceCall<Object[], RecordReader> sourceCall) {
    Map<String,ByteBuffer> key = (Map<String, ByteBuffer>) sourceCall.getInput().createKey();
    Map<String,ByteBuffer> value = (Map<String, ByteBuffer>) sourceCall.getInput().createValue();

    Object[] obj = new Object[]{key, value};

    sourceCall.setContext(obj);
  }

  /**
   *
   * @param flowProcess
   * @param sourceCall
   * @return
   * @throws IOException
   */
  @Override
  public boolean source(FlowProcess<JobConf> flowProcess,
                        SourceCall<Object[], RecordReader> sourceCall) throws IOException {
    Tuple result = new Tuple();

    RecordReader input = sourceCall.getInput();

    Map<String, String> dataTypes = SettingsHelper.getTypes(settings);
    Map<String,ByteBuffer> keys = (Map<String, ByteBuffer>) sourceCall.getContext()[0];
    Map<String,ByteBuffer> columns = (Map<String, ByteBuffer>) sourceCall.getContext()[1];

    boolean hasNext = input.next(keys, columns);

    if (!hasNext) {
      return false;
    }

    for(Map.Entry<String, ByteBuffer> key : keys.entrySet()) {
      try {
        result.add(SerializerHelper.deserialize(key.getValue(), SerializerHelper.inferType(dataTypes.get(key.getKey()))));
      } catch (Exception e) {
        throw new RuntimeException("Couldn't deserialize key: " + key.getKey(), e);
      }
    }

    for(Map.Entry<String, ByteBuffer> column : columns.entrySet()) {
      try {
        result.add(SerializerHelper.deserialize(column.getValue(), SerializerHelper.inferType(dataTypes.get(column.getKey()))));
      } catch (Exception e) {
        throw new RuntimeException("Couldn't deserialize value for: " + column.getKey(), e);
      }
    }

    sourceCall.getIncomingEntry().setTuple(result);
    return true;
  }

  /**
   *
   * @param flowProcess
   * @param sinkCall
   * @throws IOException
   */
  public void sink(FlowProcess<JobConf> flowProcess,
                   SinkCall<Object[], OutputCollector> sinkCall) throws IOException {

  }
}