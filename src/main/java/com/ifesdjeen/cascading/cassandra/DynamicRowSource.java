package com.ifesdjeen.cascading.cassandra;

import java.util.*;
import java.nio.ByteBuffer;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.tuple.Tuple;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.ifesdjeen.cascading.cassandra.hadoop.SerializerHelper;


public class DynamicRowSource
    implements ISource {

    private static final Logger logger = LoggerFactory.getLogger(StaticRowSink.class);

    public void source( Map<String, Object> settings,
                        SortedMap<ByteBuffer, IColumn> columns,
                        Tuple result ) throws IOException {

        Map<String, String> dataTypes = Util.getSourceTypes(settings);

        if (!dataTypes.isEmpty()) {

            if (columns.values().isEmpty()) {
                logger.info("Values are empty.");
            }

            for (IColumn column : columns.values()) {

                String columnName = ByteBufferUtil.string(column.name());

                try {
                    Object val = SerializerHelper.deserialize(column.name(), dataTypes.get("key"));
                    result.add(val);
                    val = SerializerHelper.deserialize(column.value(), dataTypes.get("value"));
                    result.add(val);
                } catch (Exception e) {}

            }
        } else {
            result.add(columns);
            logger.debug("No data types given. Assuming custom deserizliation.");
        }
    }
}
