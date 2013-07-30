package com.ifesdjeen.cascading.cassandra.cql3;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.ifesdjeen.cascading.cassandra.BaseCassandraScheme;
import com.ifesdjeen.cascading.cassandra.SettingsHelper;
import com.ifesdjeen.cascading.cassandra.hadoop.SerializerHelper;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.jsoup.helper.StringUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class CassandraCQL3Scheme extends BaseCassandraScheme {

  public CassandraCQL3Scheme(Map<String, Object> settings) {
    super(settings);
  }

  /**
   *
   * Source Methods
   *
   */

  /**
   *
   * @param process
   * @param tap
   * @param conf
   */
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
        result.add(SerializerHelper.deserialize(key.getValue(),
                SerializerHelper.inferType(dataTypes.get(key.getKey()))));
      } catch (Exception e) {
        throw new RuntimeException("Couldn't deserialize key: " + key.getKey(), e);
      }
    }

    for(Map.Entry<String, ByteBuffer> column : columns.entrySet()) {
      try {
        result.add(SerializerHelper.deserialize(column.getValue(),
                SerializerHelper.inferType(dataTypes.get(column.getKey()))));
      } catch (Exception e) {
        throw new RuntimeException("Couldn't deserialize value for: " + column.getKey(), e);
      }
    }

    sourceCall.getIncomingEntry().setTuple(result);
    return true;
  }

  /**
   *
   * Sink Methods
   *
   */

  /**
   *
   * @param process
   * @param tap
   * @param conf
   */
  @Override
  public void sinkConfInit(FlowProcess<JobConf> process,
                           Tap<JobConf, RecordReader, OutputCollector> tap,
                           JobConf conf) {
    super.sinkConfInit(process, tap, conf);
    conf.setOutputFormat(CqlOutputFormat.class);

    if (this.settings.containsKey("mappings.cqlKeys")) {
      List<String> keyMappings = (List<String>) this.settings.get("mappings.cqlKeys");
      conf.set("row_key", StringUtil.join(keyMappings, ","));
    } else {
      throw new RuntimeException("Can't sink without 'mappings.cqlKeys'");
    }


    if (this.settings.containsKey("sink.outputCQL")) {
      CqlConfigHelper.setOutputCql(conf, (String) this.settings.get("sink.outputCQL"));
    } else {
      throw new RuntimeException("Can't sink without 'sink.outputCQL'");
    }
  }

  /**
   *
   * @param flowProcess
   * @param sinkCall
   * @throws IOException
   */
  public void sink(FlowProcess<JobConf> flowProcess,
                   SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
    TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
    OutputCollector outputCollector = sinkCall.getOutput();

    List<String> cqlKeys = (List<String>) this.settings.get("mappings.cqlKeys");
    List<String> cqlValues = (List<String>) this.settings.get("mappings.cqlValues");
    Map<String, String> columnMappings = (Map<String, String>) this.settings.get("mappings.cql");

    Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
    List<ByteBuffer> values = new ArrayList<ByteBuffer>();

    for (String key: cqlKeys) {
      keys.put(key, SerializerHelper.serialize(tupleEntry.getObject(columnMappings.get(key))));
    }

    // Unfortunately we can't just use hashmap because of order-dependent CQL queries. OP
    for (String value: cqlValues) {
      values.add(SerializerHelper.serialize(tupleEntry.getObject(columnMappings.get(value))));
    }

    logger.debug("Keys {}", keys);

    outputCollector.collect(keys, values);
  }
}