package com.clojurewerkz.cascading.cassandra;


import cascading.tap.SinkMode;

import cascading.flow.FlowProcess;

import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;

import cascading.tuple.TupleEntrySchemeCollector;
import cascading.tuple.TupleEntrySchemeIterator;

import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import java.util.Properties;
// import org.apache.hadoop.mapred.Properties;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.OutputCollector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;


public class CassandraTap extends Tap<Properties, RecordReader, OutputCollector> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraTap.class);

    private final String id = UUID.randomUUID().toString();

    private String columnFamilyName;
    private String keyspace;

    public CassandraTap(String keyspace, String columnFamilyName, CassandraScheme scheme) {
        this(keyspace, columnFamilyName, scheme, SinkMode.UPDATE);
    }

    public CassandraTap(String keyspace, String columnFamilyName, CassandraScheme scheme, SinkMode sinkMode) {
        super(scheme, sinkMode);
        LOG.info("Initializing Cassandra Tap {} {}", keyspace, columnFamilyName);
        this.keyspace = keyspace;
        this.columnFamilyName = columnFamilyName;
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<Properties> jobConfFlowProcess, RecordReader recordReader) throws IOException {
        LOG.info("Opening for read");
        return new TupleEntrySchemeIterator<Properties, RecordReader>(jobConfFlowProcess, getScheme(), recordReader);
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<Properties> jobConfFlowProcess, OutputCollector outputCollector) throws IOException {
        LOG.info("Opening for write");
        return null;
    }

    @Override
    public String toString() {
        return "";
    }

    @Override
    public long getModifiedTime(Properties jobConf) throws IOException {
        return System.currentTimeMillis();
    }

    @Override
    public boolean resourceExists(Properties jobConf) throws IOException {
        LOG.info("Checking for resource existence");
        return true;
    }

    @Override
    public boolean deleteResource(Properties jobConf) throws IOException {
        LOG.info("Deleting resource");
        return true;
    }

    @Override
    public boolean createResource(Properties jobConf) throws IOException {
        LOG.info("Creating resource");
        return true;
    }


    @Override
    public void sourceConfInit(FlowProcess<Properties> process, Properties conf) {
        LOG.info("Initializing source conf");

    }

    public void sinkConfInit(FlowProcess<Properties> process, Properties conf) {
        LOG.info("Initializing sink conf");
    }
}
