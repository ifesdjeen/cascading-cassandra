package com.ifesdjeen.cascading.cassandra;

import cascading.tap.Tap;

import cascading.flow.FlowProcess;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.OutputCollector;

import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;

import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntryCollector;

import com.ifesdjeen.cascading.cassandra.hadoop.CassandraCollector;

import java.io.IOException;

public class CassandraTap extends Tap<JobConf, RecordReader, OutputCollector> {

  public final String id = "TEMP_ID";

  public BaseCassandraScheme scheme;

  public CassandraTap(BaseCassandraScheme scheme) {
    super(scheme);
    this.scheme = scheme;
  }

  @Override
  public String getIdentifier() {
    return id + "_" + scheme.getIdentifier();
  }

  @Override
  public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess, RecordReader recordReader) throws IOException {
    return new HadoopTupleEntrySchemeIterator(flowProcess, this, recordReader);
  }

  @Override
  public TupleEntryCollector openForWrite(FlowProcess<JobConf> flowProcess, OutputCollector outputCollector) throws IOException {
    CassandraCollector cassandraCollector = new CassandraCollector(flowProcess, this);

    cassandraCollector.prepare();
    return cassandraCollector;
  }


  @Override
  public boolean createResource(JobConf jobConf) throws IOException {
    // TODO
    return true;
  }


  @Override
  public boolean deleteResource(JobConf jobConf) throws IOException {
    // TODO
    return true;
  }


  @Override
  public boolean resourceExists(JobConf jobConf) throws IOException {
    // TODO check if column-family exists
    return true;
  }


  @Override
  public long getModifiedTime(JobConf jobConf) throws IOException {
    // TODO could read this from tables
    return System.currentTimeMillis(); // currently unable to find last mod time
    // on a table
  }

  @Override
  public boolean equals(Object other) {
    if (this == other)
      return true;
    if (!(other instanceof CassandraTap))
      return false;
    if (!super.equals(other))
      return false;

    CassandraTap otherTap = (CassandraTap) other;
    if (!otherTap.getIdentifier().equals(getIdentifier())) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + getIdentifier().hashCode();

    return result;
  }
}
