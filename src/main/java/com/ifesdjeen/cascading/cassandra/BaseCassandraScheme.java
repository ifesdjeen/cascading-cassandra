package com.ifesdjeen.cascading.cassandra;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SourceCall;

import cascading.tap.Tap;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;

import org.apache.cassandra.hadoop.ConfigHelper;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseCassandraScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

  protected static final Logger logger = LoggerFactory.getLogger(CassandraScheme.class);

  protected String pathUUID;
  protected Map<String, Object> settings;

  protected String host;
  protected String port;
  protected String columnFamily;
  protected String keyspace;

  public BaseCassandraScheme(Map<String, Object> settings) {
    this.settings = settings;
    this.pathUUID = UUID.randomUUID().toString();

    if (this.settings.containsKey("db.port")) {
      this.port = (String) this.settings.get("db.port");
    } else {
      this.port = "9160";
    }

    if (this.settings.containsKey("db.host")) {
      this.host = (String) this.settings.get("db.host");
    } else {
      this.host = "localhost";
    }

    this.keyspace = (String) this.settings.get("db.keyspace");
    this.columnFamily = (String) this.settings.get("db.columnFamily");
  }

  /**
   *
   * Source Methods
   *
   */

  @Override
  public void sourceConfInit(FlowProcess<JobConf> process,
                             Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    logger.info("Configuring source...");

    ConfigHelper.setInputRpcPort(conf, port);
    ConfigHelper.setInputInitialAddress(conf, this.host);

    if (this.settings.containsKey("source.rangeBatchSize")) {
      ConfigHelper.setRangeBatchSize(conf, (Integer) this.settings.get("source.rangeBatchSize"));
    }

    if (this.settings.containsKey("source.inputSplitSize")) {
      ConfigHelper.setInputSplitSize(conf, (Integer) this.settings.get("source.inputSplitSize"));
    }

    if (this.settings.containsKey("cassandra.inputPartitioner")) {
      ConfigHelper.setInputPartitioner(conf, (String) this.settings.get("cassandra.inputPartitioner"));
    } else {
      ConfigHelper.setInputPartitioner(conf, "org.apache.cassandra.dht.Murmur3Partitioner");
    }

    FileInputFormat.addInputPaths(conf, getPath().toString());
  }


  /**
   * @param flowProcess
   * @param sourceCall
   */
  @Override
  public void sourceCleanup(FlowProcess<JobConf> flowProcess,
                            SourceCall<Object[], RecordReader> sourceCall) {
    sourceCall.setContext(null);
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
    ConfigHelper.setRangeBatchSize(conf, 1000);

    ConfigHelper.setOutputRpcPort(conf, port);
    ConfigHelper.setOutputInitialAddress(conf, host);

    if (this.settings.containsKey("cassandra.outputPartitioner")) {
      ConfigHelper.setOutputPartitioner(conf, (String) this.settings.get("cassandra.outputPartitioner"));
    } else {
      ConfigHelper.setOutputPartitioner(conf, "org.apache.cassandra.dht.Murmur3Partitioner");
    }

    ConfigHelper.setOutputColumnFamily(conf, keyspace, columnFamily);

    FileOutputFormat.setOutputPath(conf, getPath());
  }

  /**
   *
   * Generic Methods
   *
   */

  public Path getPath() {
    return new Path(pathUUID);
  }

  public String getIdentifier() {
    return host + "_" + port + "_" + keyspace + "_" + columnFamily;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other)
      return true;
    if (!(other instanceof CassandraScheme))
      return false;
    if (!super.equals(other))
      return false;

    CassandraScheme that = (CassandraScheme) other;

    if (!getPath().toString().equals(that.getPath().toString()))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + getPath().toString().hashCode();
    result = 31 * result + (host != null ? host.hashCode() : 0);
    result = 31 * result + (port != null ? port.hashCode() : 0);
    result = 31 * result + (keyspace != null ? keyspace.hashCode() : 0);
    result = 31 * result + (columnFamily != null ? columnFamily.hashCode() : 0);
    return result;
  }

}
