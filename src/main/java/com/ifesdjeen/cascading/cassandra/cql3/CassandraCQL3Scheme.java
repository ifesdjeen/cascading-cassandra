package com.ifesdjeen.cascading.cassandra.cql3;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.ifesdjeen.cascading.cassandra.BaseCassandraScheme;
import com.ifesdjeen.cascading.cassandra.SettingsHelper;
import com.ifesdjeen.cascading.cassandra.sinks.CqlSink;
import com.ifesdjeen.cascading.cassandra.sinks.ISink;
import com.ifesdjeen.cascading.cassandra.sources.CqlSource;
import com.ifesdjeen.cascading.cassandra.sources.ISource;
import org.apache.cassandra.hadoop2.ConfigHelper;
import org.apache.cassandra.hadoop2.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop2.cql3.CqlOutputFormat;
import org.apache.cassandra.hadoop2.cql3.CqlPagingInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.*;

public class CassandraCQL3Scheme extends BaseCassandraScheme {

  protected CqlSource sourceImpl;
  protected CqlSink sinkImpl;

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

    String sourceColumns = SettingsHelper.getSourceColumns(this.settings);
    if (sourceColumns != null) {
      CqlConfigHelper.setInputColumns(conf, sourceColumns);
    }

    if (this.settings.containsKey("source.CQLPageRowSize")) {
      CqlConfigHelper.setInputCQLPageRowSize(conf, (String) this.settings.get("source.CQLPageRowSize"));
    } else {
      CqlConfigHelper.setInputCQLPageRowSize(conf, "10000");
    }

    if (this.settings.containsKey("source.whereClauses")) {
      CqlConfigHelper.setInputWhereClauses(conf, (String) this.settings.get("source.whereClauses"));
    }

    conf.set("row_key", "name");

    sourceImpl = new CqlSource();
    sourceImpl.configure(this.settings);
  }

  /**
   *
   * @param flowProcess
   * @param sourceCall
   */
  @Override
  public void sourcePrepare(FlowProcess<JobConf> flowProcess,
                            SourceCall<Object[], RecordReader> sourceCall) {
    sourceImpl.sourcePrepare(sourceCall);
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
    RecordReader input = sourceCall.getInput();

    Object keys = sourceCall.getContext()[0];
    Object columns = sourceCall.getContext()[1];

    boolean hasNext = input.next(keys, columns);

    if (!hasNext) {
      return false;
    }

    Tuple result = sourceImpl.source(keys, columns);
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
      conf.set("row_key", StringUtils.join(keyMappings, ","));
    } else {
      throw new RuntimeException("Can't sink without 'mappings.cqlKeys'");
    }


    if (this.settings.containsKey("sink.outputCQL")) {
      CqlConfigHelper.setOutputCql(conf, (String) this.settings.get("sink.outputCQL"));
    } else {
      CqlConfigHelper.setOutputCql(conf, SettingsHelper.getSinkOutputCql( this.settings ));
    }

    sinkImpl = new CqlSink();
    sinkImpl.configure(this.settings);
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

    sinkImpl.sink(tupleEntry, outputCollector);
  }
}
