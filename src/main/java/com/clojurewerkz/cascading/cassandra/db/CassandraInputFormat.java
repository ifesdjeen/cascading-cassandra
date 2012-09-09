package com.clojurewerkz.cascading.cassandra.db;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.Override;
import java.lang.String;

public class CassandraInputFormat<T extends CassandraWritable> implements InputFormat<LongWritable, T>, JobConfigurable {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraInputFormat.class);

    protected class CassandrRecordReader implements RecordReader<LongWritable, T> {
        @Override
        public boolean next(LongWritable longWritable, T t) throws IOException {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public LongWritable createKey() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public T createValue() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public long getPos() throws IOException {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void close() throws IOException {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public float getProgress() throws IOException {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }
    }

    /** A InputSplit that spans a set of rows */
    protected static class DBInputSplit implements InputSplit {

        @Override
        public long getLength() throws IOException {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String[] getLocations() throws IOException {
            return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }

    @Override
    public InputSplit[] getSplits(JobConf entries, int i) throws IOException {
        LOG.info("Gettin splits...");
        return new InputSplit[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public RecordReader<LongWritable, T> getRecordReader(InputSplit inputSplit, JobConf entries, Reporter reporter) throws IOException {
        LOG.info("getRecordReader...");
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void configure(JobConf entries) {
        LOG.info("configure...");
        //To change body of implemented methods use File | Settings | File Templates.
    }

}
