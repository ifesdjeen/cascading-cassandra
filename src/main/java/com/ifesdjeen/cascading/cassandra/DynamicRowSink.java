package com.ifesdjeen.cascading.cassandra;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.FieldsResolverException;


import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.thrift.*;

import com.ifesdjeen.cascading.cassandra.hadoop.SerializerHelper;

import java.io.IOException;
import java.util.*;
import java.nio.ByteBuffer;

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicRowSink
    implements ISink {

    private static final Logger logger = LoggerFactory.getLogger(DynamicRowSink.class);

    public List<Mutation> sink( Map<String, Object> settings,
                                TupleEntry tupleEntry ) throws IOException {

        Map<String, String> dataTypes = SettingsHelper.getDynamicTypes(settings);
        Map<String, String> dynamicMappings = SettingsHelper.getDynamicMappings(settings);

        AbstractType columnNameType = null;
        try {
            columnNameType = SerializerHelper.inferType(dataTypes.get("rowKey"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<String> columnNameFields = new ArrayList<String>();
        Object columnNameFieldSpec = dynamicMappings.get("columnName");
        if (columnNameFieldSpec instanceof String) {
            // simple column name
            columnNameFields.add((String)columnNameFieldSpec);
        } else {
            // composite column name
            for(String columnNameField : (List<String>)columnNameFieldSpec) {
                columnNameFields.add(columnNameField);
            }
        }

        String columnValueField = (String) dynamicMappings.get("columnValue");

        List<Mutation> mutations = new ArrayList<Mutation>();

        List tupleEntryColumnNameValues = new ArrayList();
        for(String columnNameField : columnNameFields) {
            try {
                tupleEntryColumnNameValues.add( tupleEntry.get(columnNameField) );
            } catch (FieldsResolverException e) {
                throw new RuntimeException("Couldn't resolve column name field: " + columnNameField);
            }
        }

        Object tupleEntryColumnValueValue = null;
        try {
            tupleEntryColumnValueValue = tupleEntry.get(columnValueField);
        } catch (FieldsResolverException e) {
            throw new RuntimeException("Couldn't resolve column value field: " + columnValueField);
        }

        if (tupleEntryColumnNameValues.size() > 0) {
            logger.info("Mapped column name fields {}", columnNameFields.toArray());
            logger.info("column name value {}", tupleEntryColumnNameValues.toArray());
            logger.info("Mapped column value field {}", columnValueField);
            logger.info("Column value value {}", tupleEntryColumnValueValue);

            ByteBuffer columnName = null;
            if (columnNameType != null && columnNameType instanceof CompositeType) {
                columnName = SerializerHelper.serializeComposite(tupleEntryColumnNameValues, (CompositeType)columnNameType);
            } else {
                columnName = SerializerHelper.serialize(tupleEntryColumnNameValues.get(0));
            }

            Mutation mutation = Util.createColumnPutMutation(columnName,
                                                             SerializerHelper.serialize(tupleEntryColumnValueValue));
            mutations.add(mutation);
        }

        return mutations;
    }
}
