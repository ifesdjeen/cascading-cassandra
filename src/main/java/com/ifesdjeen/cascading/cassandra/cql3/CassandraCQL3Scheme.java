package com.ifesdjeen.cascading.cassandra.cql3;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import com.ifesdjeen.cascading.cassandra.BaseCassandraScheme;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

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
    ConfigHelper.setOutputColumnFamily(conf, this.keyspace, this.columnFamily);

    conf.setInputFormat(CqlPagingInputFormat.class);
    conf.setOutputFormat(CqlOutputFormat.class);

    CqlConfigHelper.setInputCQLPageRowSize(conf, "3");

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
    System.out.println("====================111=========================");
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
    System.out.println("====================222=========================");

    RecordReader input = sourceCall.getInput();

    Object key = sourceCall.getContext()[0];
    Object value = sourceCall.getContext()[1];

    boolean hasNext = input.next(key, value);

    System.out.println("WYF");
    logger.debug("WOWOWOW");

    if (!hasNext) {
      return false;
    }

    Map<String, ByteBuffer> columns = (Map<String, ByteBuffer>) value;

    for(Map.Entry<String, ByteBuffer> column : columns.entrySet()) {
      System.out.println(column.getKey());

    }

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