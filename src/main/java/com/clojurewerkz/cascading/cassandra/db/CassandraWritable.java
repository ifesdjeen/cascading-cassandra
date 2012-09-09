package com.clojurewerkz.cascading.cassandra.db;

import org.apache.hadoop.io.Writable;

import java.util.HashMap;

import java.io.IOException;
import java.io.DataOutput;

public interface CassandraWritable {
    public void write(DataOutput out) throws IOException;
    public void readFields(HashMap resultSet) throws IOException;
}
