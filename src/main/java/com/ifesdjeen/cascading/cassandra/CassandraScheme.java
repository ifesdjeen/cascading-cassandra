package com.ifesdjeen.cascading.cassandra;

import com.ifesdjeen.cascading.cassandra.sinks.DynamicRowSink;
import com.ifesdjeen.cascading.cassandra.sinks.ISink;
import com.ifesdjeen.cascading.cassandra.sinks.StaticRowSink;
import com.ifesdjeen.cascading.cassandra.sources.DynamicRowSource;
import com.ifesdjeen.cascading.cassandra.sources.ISource;
import com.ifesdjeen.cascading.cassandra.sources.StaticRowSource;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;

import cascading.tap.Tap;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.*;

import org.apache.cassandra.hadoop2.ConfigHelper;
import org.apache.cassandra.hadoop2.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop2.ColumnFamilyInputFormat;

import java.io.IOException;
import java.util.*;
import java.nio.ByteBuffer;

public class CassandraScheme extends BaseCassandraScheme {

  public CassandraScheme(Map<String, Object> settings) {
    super(settings);
  }

  /**
   *
   * Source Methods
   *
   */

  /**
   * @param flowProcess
   * @param sourceCall
   */
  @Override
  public void sourcePrepare(FlowProcess<JobConf> flowProcess,
                            SourceCall<Object[], RecordReader> sourceCall) {
    ISource sourceImpl = getSourceImpl();
    sourceImpl.sourcePrepare(sourceCall);
  }

  /**
   *
   * @param process
   * @param tap
   * @param conf
   */
  @Override
  public void sourceConfInit(FlowProcess<JobConf> process,
                             Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    super.sourceConfInit(process, tap, conf);

    conf.setInputFormat(ColumnFamilyInputFormat.class);

    ConfigHelper.setRangeBatchSize(conf, 1000);

    if (this.settings.containsKey("source.predicate")) {
      ConfigHelper.setInputSlicePredicate(conf, (SlicePredicate) this.settings.get("source.predicate"));
    } else {
      SlicePredicate predicate = new SlicePredicate();

      List<String> sourceColumns = this.getSourceColumns();

      if (!sourceColumns.isEmpty()) {
        logger.debug("Using with following columns: {}", StringUtils.join(sourceColumns, ","));

        List<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
        for (String columnFieldName : sourceColumns) {
          columnNames.add(ByteBufferUtil.bytes(columnFieldName));
        }

        predicate.setColumn_names(columnNames);
      } else {
        logger.debug("Using slicerange over all columns");

        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(ByteBufferUtil.bytes(""));
        sliceRange.setFinish(ByteBufferUtil.bytes(""));
        predicate.setSlice_range(sliceRange);
      }
      ConfigHelper.setInputSlicePredicate(conf, predicate);
    }

    if (this.settings.containsKey("source.useWideRows")) {
      ConfigHelper.setInputColumnFamily(conf, this.keyspace, this.columnFamily,
              (Boolean) this.settings.get("source.useWideRows"));
    } else {
      ConfigHelper.setInputColumnFamily(conf, this.keyspace, this.columnFamily);
    }


  }

  /**
   * FIXME: Pitfalls: Currently only String is supported as a rowKey.
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

    Object key = sourceCall.getContext()[0];
    Object columns = sourceCall.getContext()[1];

    boolean hasNext = input.next(key, columns);

    if (!hasNext) {
      return false;
    }

    ISource sourceImpl = getSourceImpl();
    Tuple result = sourceImpl.source(this.settings, key, columns);

    sourceCall.getIncomingEntry().setTuple(result);
    return true;
  }

  protected List<String> getSourceColumns() {
    if (this.settings.containsKey("source.columns")) {
      return (List<String>) this.settings.get("source.columns");
    } else {
      return new ArrayList<String>();
    }
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

    if (this.settings.containsKey("sink.outputFormat")) {
      String outputFormatName = (String)this.settings.get("sink.outputFormat");
      try {
        Class outputFormat = (Class<? extends OutputFormat>)Class.forName(outputFormatName);
        conf.setOutputFormat(outputFormat);
      }
      catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Class not found: " + outputFormatName, e);
      }
    }
    else {
      conf.setOutputFormat(ColumnFamilyOutputFormat.class);
    }
  }

  /**
   * @param flowProcess
   * @param sinkCall
   * @throws IOException
   */
  @Override
  public void sink(FlowProcess<JobConf> flowProcess,
                   SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
    TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
    OutputCollector outputCollector = sinkCall.getOutput();

    ISink sinkImpl = getSinkImpl();
    sinkImpl.sink(settings, tupleEntry, outputCollector);
  }

  protected ISink getSinkImpl() {
    String className = (String) this.settings.get("sink.sinkImpl");
    boolean useWideRows = SettingsHelper.isDynamicMapping(this.settings);

    try {
      if (className == null) {
        if (useWideRows) {
          return new DynamicRowSink();
        } else {
          return new StaticRowSink();
        }
      } else {
        Class<ISink> klass = (Class<ISink>) Class.forName(className);
        return klass.newInstance();
      }
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

  }

  protected ISource getSourceImpl() {
    String className = (String) this.settings.get("source.sourceImpl");
    boolean useWideRows = SettingsHelper.isDynamicMapping(this.settings);

    try {
      if (className == null) {
        if (useWideRows) {
          return new DynamicRowSource();
        } else {
          return new StaticRowSource();
        }
      } else {
        Class<ISource> klass = (Class<ISource>) Class.forName(className);
        return klass.newInstance();
      }
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
