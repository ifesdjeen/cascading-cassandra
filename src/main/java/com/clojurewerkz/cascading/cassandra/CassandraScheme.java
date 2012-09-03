package com.clojurewerkz.cascading.cassandra;

import java.io.IOException;

import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;

import java.util.Properties;
// import org.apache.hadoop.mapred.Properties;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.clojurewerkz.cascading.cassandra.CassandraSink;
import com.clojurewerkz.cascading.cassandra.CassandraTap;

public class CassandraScheme extends Scheme<Properties, RecordReader, OutputCollector, Object[], Object[]> {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraScheme.class);

  public CassandraScheme(Fields valueFields, String columnFamilyName) {

  }

  @Override
  public void sourceConfInit(FlowProcess<Properties> process, Tap<Properties, RecordReader, OutputCollector> tap, Properties conf) {
      LOG.info("Initializing Schema Source for");
  }

  @Override
  public void sinkConfInit(FlowProcess<Properties> process, Tap<Properties, RecordReader, OutputCollector> tap, Properties conf) {
      LOG.info("Initializing Schema Sink for");
  }

  @Override
  public boolean source(FlowProcess<Properties> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
      LOG.info("Running source");
      return CassandraSource.source(flowProcess, sourceCall);
  }

  @Override
  public void sink(FlowProcess<Properties> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
      LOG.info("Running sink");

  }

  @Override
  public String toString() {
      return getClass().getSimpleName();
  }

}
