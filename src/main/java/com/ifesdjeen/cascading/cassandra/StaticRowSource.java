package com.ifesdjeen.cascading.cassandra;

import java.util.*;
import java.nio.ByteBuffer;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.tuple.Tuple;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.ifesdjeen.cascading.cassandra.hadoop.SerializerHelper;


public class StaticRowSource
    implements ISource {

    private static final Logger logger = LoggerFactory.getLogger(StaticRowSource.class);

    public void source( Map<String, Object> settings,
                        SortedMap<ByteBuffer, IColumn> columns,
                        Tuple result ) throws IOException {

        Map<String, String> dataTypes = SettingsHelper.getTypes(settings);
        List<String> sourceMappings = SettingsHelper.getSourceMappings(settings);

        if (columns.values().isEmpty()) {
            logger.info("Values are empty.");
        }

        Map<String, IColumn> columnsByStringName = new HashMap<String, IColumn>();
        for (IColumn column : columns.values()) {
            columnsByStringName.put( ByteBufferUtil.string( column.name() ),
                                     column );
        }

        for(String columnName : sourceMappings) {
            AbstractType columnValueType = SerializerHelper.inferType( dataTypes.get(columnName) );
            if (columnValueType != null) {
                try {
                    IColumn column = columnsByStringName.get(columnName);
                    Object val = SerializerHelper.deserialize(column.value(), columnValueType);
                    logger.debug("Putting deserialized column: {}. {}", columnName, val);
                    result.add(val);
                } catch (Exception e) {
                    throw new RuntimeException("Couldn't deserialize column: {}. {}" + columnName, e);
                }
            } else {
                throw new RuntimeException( "no type given for column: " + columnName );
            }

        }
    }
}
