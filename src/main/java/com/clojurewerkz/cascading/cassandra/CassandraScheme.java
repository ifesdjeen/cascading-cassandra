package com.clojurewerkz.cascading.cassandra;

import java.io.IOException;

import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.clojurewerkz.cascading.cassandra.CassandraSink;
import com.clojurewerkz.cascading.cassandra.CassandraTap;

public class CassandraScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraScheme.class);

  public CassandraScheme(Fields valueFields, String columnFamilyName) {
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> process, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> process, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
  }

  @Override
  public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
      return CassandraSource.source(flowProcess, sourceCall);
  }

  @Override
  public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {

  }

  @Override
  public String toString() {
      return getClass().getSimpleName();
  }

}
