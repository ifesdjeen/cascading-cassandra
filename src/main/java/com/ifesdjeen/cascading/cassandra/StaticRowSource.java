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


public class StaticRowSource
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

                if (dataTypes.containsKey(columnName)) {
                    try {
                        Object val = SerializerHelper.deserialize(column.value(), dataTypes.get(columnName));
                        logger.debug("Putting deserialized column: {}. {}", columnName, val);
                        result.add(val);
                    } catch (Exception e) {
                        logger.error("Couldn't deserialize column: {}. {}", columnName, e.getMessage());
                    }
                } else {
                    logger.info("Skipping column, because there was no type given: {}", columnName);
                }
            }
        } else {
            result.add(columns);
            logger.debug("No data types given. Assuming custom deserizliation.");
        }
    }
}
