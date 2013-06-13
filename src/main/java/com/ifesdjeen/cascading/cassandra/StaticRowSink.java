package com.ifesdjeen.cascading.cassandra;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.FieldsResolverException;
import org.apache.cassandra.thrift.*;

import com.ifesdjeen.cascading.cassandra.hadoop.SerializerHelper;

import java.io.IOException;
import java.util.*;
import java.nio.ByteBuffer;

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticRowSink
    implements ISink {

    private static final Logger logger = LoggerFactory.getLogger(StaticRowSink.class);

    public List<Mutation> sink( Map<String, Object> settings,
                                TupleEntry tupleEntry ) {

        List<String> columnFieldNames = getSourceColumns(settings);
        int nfields = columnFieldNames.size();

        Map<String, String> fieldMappings = (Map<String, String>) settings.get("sink.outputMappings");

        String keyColumnName = (String) settings.get("sink.keyColumnName");

        List<Mutation> mutations = new ArrayList<Mutation>(nfields);

        for (String columnFieldName : columnFieldNames) {
            String columnFieldMapping = fieldMappings.get(columnFieldName);
            Object tupleEntryValue = null;

            try {
                tupleEntryValue = tupleEntry.get(columnFieldMapping);
            } catch (FieldsResolverException e) {
                logger.error("Couldn't resolve field: {}", columnFieldName);
            }

            if (tupleEntryValue != null && columnFieldName != keyColumnName) {
                logger.info("Column filed name {}", columnFieldName);
                logger.info("Mapped column name {}", columnFieldMapping);
                logger.info("Column filed value {}", tupleEntry.get(columnFieldMapping));

                Mutation mutation = Util.createColumnPutMutation(SerializerHelper.serialize(columnFieldName),
                                                                 SerializerHelper.serialize(tupleEntry.get(columnFieldMapping)));
                mutations.add(mutation);
            }
        }

        return mutations;
    }

    private List<String> getSourceColumns( Map<String, Object> settings) {
        if (settings.containsKey("source.columns")) {
            return (List<String>) settings.get("source.columns");
        } else {
            return new ArrayList<String>();
        }
    }


}
