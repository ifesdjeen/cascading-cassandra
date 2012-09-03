package com.clojurewerkz.cascading.cassandra;


import cascading.tap.SinkMode;

import cascading.flow.FlowProcess;

import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;

import cascading.tap.hadoop.io.HadoopTupleEntrySchemeCollector;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;

import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.OutputCollector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;


public class CassandraTap extends Tap<JobConf, RecordReader, OutputCollector> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraTap.class);

    private final String id = UUID.randomUUID().toString();

    private String columnFamilyName;
    private String keyspace;

    public CassandraTap(String keyspace, String columnFamilyName, CassandraScheme scheme) {
        this(keyspace, columnFamilyName, scheme, SinkMode.UPDATE);
    }

    public CassandraTap(String keyspace, String columnFamilyName, CassandraScheme scheme, SinkMode sinkMode) {
        super(scheme, sinkMode);
        LOG.info("Created Tap");

        this.keyspace = keyspace;
        this.columnFamilyName = columnFamilyName;
    }

    @Override
    public boolean isSink()
    {
        LOG.info("is Sink?");

        return true;
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public void sourceConfInit( FlowProcess<JobConf> process, JobConf conf ) {
        LOG.info("Source conf init");
    }

    @Override
    public void sinkConfInit( FlowProcess<JobConf> process, JobConf conf ) {
        LOG.info("Sink conf init");
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<JobConf> jobConfFlowProcess, RecordReader recordReader) throws IOException {
        LOG.info("Opening for read");
        return new HadoopTupleEntrySchemeIterator(jobConfFlowProcess, this, recordReader);
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<JobConf> jobConfFlowProcess, OutputCollector outputCollector) throws IOException {
        LOG.info("Open For Write");

        return null;
    }

    @Override
    public String toString() {
        return "";
    }

    @Override
    public long getModifiedTime(JobConf jobConf) throws IOException {
        return System.currentTimeMillis();
    }

    @Override
    public boolean resourceExists(JobConf jobConf) throws IOException {
        LOG.info("Checking resource Existence");

        return true;
    }

    @Override
    public boolean deleteResource(JobConf jobConf) throws IOException {
        LOG.info("Deleting resource");

        return true;
    }

    @Override
    public boolean createResource(JobConf jobConf) throws IOException {
        LOG.info("Creating resource");

        return true;
    }
}
