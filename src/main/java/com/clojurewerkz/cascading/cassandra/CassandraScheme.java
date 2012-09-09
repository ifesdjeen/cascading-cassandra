package com.clojurewerkz.cascading.cassandra;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;

import com.clojurewerkz.cascading.cassandra.hadoop.ColumnFamilyInputFormat;
import com.clojurewerkz.cascading.cassandra.hadoop.ColumnFamilyOutputFormat;
import com.clojurewerkz.cascading.cassandra.hadoop.ConfigHelper;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.SortedMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clojurewerkz.cascading.cassandra.CassandraSource;

import org.apache.cassandra.db.IColumn;

public class CassandraScheme
    extends Scheme<JobConf, RecordReader, OutputCollector, Object[],
Object[]> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraTap.class);

    private String pathUUID;
    private String host;
    private String port;
    private String keyspace;
    private String columnFamily;
    private List<String> columnFieldNames;


    public CassandraScheme(String host, String port, String keyspace,
                           String columnFamily, List<String> columnFieldNames) {

      this.host = host;
      this.port = port;
      this.keyspace = keyspace;
      this.columnFamily = columnFamily;
      this.columnFieldNames = columnFieldNames;

      this.pathUUID = UUID.randomUUID().toString();
      //setSourceFields(new Fields("text3")); // default is unknown
      //setSinkFields
  }

  @Override
  public void sourcePrepare(FlowProcess<JobConf> flowProcess,
      SourceCall<Object[], RecordReader> sourceCall) {

      ByteBuffer key = ByteBufferUtil.clone((ByteBuffer)sourceCall.getInput().createKey());
      SortedMap<ByteBuffer, IColumn> value = (SortedMap<ByteBuffer, IColumn>)sourceCall.getInput().createValue();

      Object[] obj = new Object[]{key, value};

    sourceCall.setContext(obj);
  }

  @Override
  public void sourceCleanup(FlowProcess<JobConf> flowProcess,
                            SourceCall<Object[], RecordReader> sourceCall) {
      sourceCall.setContext(null);
  }

  @Override
  public boolean source(FlowProcess<JobConf> flowProcess,
                        SourceCall<Object[], RecordReader> sourceCall) throws IOException {
      // return CassandraSource.source(flowProcess, sourceCall);

    Tuple result = new Tuple();

    Object key = sourceCall.getContext()[0];
    Object value = sourceCall.getContext()[1];

    ByteBuffer rowkey = ByteBufferUtil.clone((ByteBuffer)key);
    boolean hasNext = sourceCall.getInput().next(rowkey, value);

    // boolean hasNext = sourceCall.getInput().nextKeyValue();
    if (!hasNext) { return false; }

    SortedMap<ByteBuffer, IColumn> columns = (SortedMap<ByteBuffer, IColumn>) value;

    result.add(ByteBufferUtil.string(rowkey));

    for (String columnFieldName: columnFieldNames) {
        IColumn col = columns.get(ByteBufferUtil.bytes(columnFieldName));
        if (col != null) {
            result.add(ByteBufferUtil.string(col.value()));
        } else {
            result.add(" ");
        }
    }
    sourceCall.getIncomingEntry().setTuple(result);
    return true;

  }

  protected ByteBuffer serialize(Object obj) {
      // BytesWritable bw = (BytesWritable) obj;
      // return ByteBuffer.wrap(bw.getBytes(), 0, bw.getLength());
      return ByteBufferUtil.bytes((String) obj);
    }
  @Override
  public void sink(FlowProcess<JobConf> flowProcess,
                   SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
      System.out.println("sink");

      TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
      OutputCollector outputCollector = sinkCall.getOutput();

      Tuple key = tupleEntry.selectTuple(new Fields("?value1"));
      ByteBuffer keyBuffer =  serialize(key.get(0));

      int nfields = columnFieldNames.size();
      List mutations = new ArrayList<Mutation>(nfields);

      for (String columnFieldName: columnFieldNames) {
          LOG.info("Column filed name {}", columnFieldName);
          LOG.info("Column filed value {}", tupleEntry.get(columnFieldName));
          Mutation mutation = createColumnPutMutation(serialize(columnFieldName),
                                                      serialize(tupleEntry.get(columnFieldName)));
          mutations.add(mutation);
      }

      outputCollector.collect(keyBuffer, mutations);
  }

    protected Mutation createColumnPutMutation(
            ByteBuffer name, ByteBuffer value) {
        Column column = new Column(name);
        column.setName(name);
        column.setValue(value);
        column.setTimestamp(System.currentTimeMillis());

        Mutation m = new Mutation();
        ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
        columnOrSuperColumn.setColumn(column);
        m.setColumn_or_supercolumn(columnOrSuperColumn);

        return m;
    }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> process,
                           Tap<JobConf, RecordReader, OutputCollector> tap,
                           JobConf conf) {
    conf.setOutputFormat(ColumnFamilyOutputFormat.class);

    ConfigHelper.setRangeBatchSize(conf, 100);

    ConfigHelper.setOutputRpcPort(conf, port);
    ConfigHelper.setOutputInitialAddress(conf, host);
    ConfigHelper.setOutputPartitioner(conf, "org.apache.cassandra.dht.ByteOrderedPartitioner");
    ConfigHelper.setOutputColumnFamily(conf, keyspace, columnFamily);
    conf.setInt(ColumnFamilyInputFormat.CASSANDRA_HADOOP_MAX_KEY_SIZE, 60);

    FileOutputFormat.setOutputPath(conf, getPath());
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> process,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {

      FileInputFormat.addInputPaths(conf, getPath().toString());
      conf.setInputFormat(ColumnFamilyInputFormat.class);

      ConfigHelper.setRangeBatchSize(conf, 100);
      ConfigHelper.setInputSplitSize(conf, 30);
      ConfigHelper.setInputRpcPort(conf, port);
      ConfigHelper.setInputInitialAddress(conf, host);
      ConfigHelper.setInputPartitioner(conf, "org.apache.cassandra.dht.ByteOrderedPartitioner");
      ConfigHelper.setInputColumnFamily(conf, keyspace, columnFamily);
      conf.setInt(ColumnFamilyInputFormat.CASSANDRA_HADOOP_MAX_KEY_SIZE, 60);

      List<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
      for (String columnFieldName: columnFieldNames) {
          columnNames.add(ByteBufferUtil.bytes(columnFieldName));
      }

      SlicePredicate predicate = new SlicePredicate().setColumn_names(columnNames);
      ConfigHelper.setInputSlicePredicate(conf, predicate);
      // ConfigHelper.setInputSplitSize(conf, 3);
  }

  public Path getPath() {
        return new Path(pathUUID);
  }

  public String getIdentifier(){
      return host + "_" + port + "_" + keyspace + "_" + columnFamily;
  }

  @Override
      public boolean equals( Object other ) {
      if( this == other )
          return true;
      if( !( other instanceof CassandraScheme ) )
          return false;
      if( !super.equals( other ) )
          return false;

      CassandraScheme that = (CassandraScheme) other;

      if(!getPath().toString().equals(that.getPath().toString()))
          return false;

      return true;
  }

  @Override
      public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + getPath().toString().hashCode();
      result = 31 * result + ( host != null ? host.hashCode() : 0 );
      result = 31 * result + ( port != null ? port.hashCode() : 0 );
      result = 31 * result + ( keyspace != null ? keyspace.hashCode() : 0 );
      result = 31 * result + ( columnFamily != null ?
columnFamily.hashCode() : 0 );
      return result;
  }

}
