package com.ifesdjeen.cascading.cassandra;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.FieldsResolverException;


import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.AbstractCompositeType;
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

        AbstractType columnNameType = SerializerHelper.inferType(dataTypes.get("columnName"));

        Object columnNameFieldSpec = dynamicMappings.get("columnName");
        List<String> columnNameFields = new ArrayList<String>();
        if (columnNameFieldSpec instanceof String) {
            columnNameFields.add((String)columnNameFieldSpec);
        } else {
            columnNameFields.addAll((List<String>)columnNameFieldSpec);
        }

        List tupleEntryColumnNameValues = new ArrayList();
        for(String columnNameField : columnNameFields) {
            try {
                Object columnNameValue = tupleEntry.getObject(columnNameField);
                logger.info("column name component field: {}", columnNameField);
                logger.info("column name component value: {}", columnNameValue);
                tupleEntryColumnNameValues.add( columnNameValue );
            } catch (FieldsResolverException e) {
                throw new RuntimeException("Couldn't resolve column name field: " + columnNameField);
            }
        }

        String columnValueField = (String) dynamicMappings.get("columnValue");
        Object tupleEntryColumnValueValue = null;
        try {
            tupleEntryColumnValueValue = tupleEntry.getObject(columnValueField);
            logger.info("column value field: {}", columnValueField);
            logger.info("column value value: {}", tupleEntryColumnValueValue);
        } catch (FieldsResolverException e) {
            throw new RuntimeException("Couldn't resolve column value field: " + columnValueField);
        }

        List<Mutation> mutations = new ArrayList<Mutation>();
        if (tupleEntryColumnNameValues.size() > 0) {

            ByteBuffer columnName = null;
            if (columnNameType instanceof CompositeType) {
                logger.info("CompositeType");
                columnName = SerializerHelper.serializeComposite(tupleEntryColumnNameValues, (CompositeType)columnNameType);
            } else {
                logger.info("SimpleType");
                columnName = SerializerHelper.serialize(tupleEntryColumnNameValues.get(0));
            }

            Mutation mutation = Util.createColumnPutMutation(columnName,
                                                             SerializerHelper.serialize(tupleEntryColumnValueValue));
            mutations.add(mutation);
        }

        return mutations;
    }
}
