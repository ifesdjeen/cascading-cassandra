package com.ifesdjeen.cascading.cassandra;

import java.util.*;

public class SettingsHelper {

  public static Map<String, String> getTypesByKey(Map<String, Object> settings, String key) {
    if (settings.containsKey(key)) {
      return (Map<String, String>) settings.get(key);
    } else {
      throw new RuntimeException("no config type specs for key: " + key);
    }
  }

    /* static types and mappings */

  public static Map<String, String> getTypes(Map<String, Object> settings) {
    return getTypesByKey(settings, "types");
  }

  public static List<String> getSourceMappings(Map<String, Object> settings) {
    Object obj = settings.get("mappings.source");
    if (obj instanceof String) {
      List<String> singleMapping = Arrays.asList((String) obj);
      return singleMapping;
    } else if (obj instanceof List) {
      return (List<String>) obj;
    } else {
      throw new RuntimeException("mappings.source must be a String or List<String>");
    }
  }

  public static Map<String, String> getSinkMappings(Map<String, Object> settings) {
    Map<String, String> sinkMappings = (Map<String, String>) settings.get("mappings.sink");
    if (sinkMappings == null) {
      throw new RuntimeException("no setting: mappings.sink");
    }
    return sinkMappings;
  }

    /* dynamic types and mappings */

  public static Map<String, String> getDynamicTypes(Map<String, Object> settings) {
    Map<String, String> dynamicTypes = getTypesByKey(settings, "types.dynamic");
    if (dynamicTypes == null) {
      throw new RuntimeException("no setting: types.dynamic");
    }
    return dynamicTypes;
  }

  public static Map<String, String> getDynamicMappings(Map<String, Object> settings) {
    Map<String, String> dynamicMappings = (Map<String, String>) settings.get("mappings.dynamic");
    if (dynamicMappings == null) {
      throw new RuntimeException("no setting: mappings.dynamic");
    }
    return dynamicMappings;
  }

  /**
   * get the field containing the row-key... different ways of getting it depending
   * on whether this is a static or dynamic mapping
   */
  public static String getMappingRowKeyField(Map<String, Object> settings) {
    if (isDynamicMapping(settings)) {
      Map<String, String> dynamicMappings = getDynamicMappings(settings);
      String rowKeyField = dynamicMappings.get("rowKey");
      if (rowKeyField == null) {
        throw new RuntimeException("must set a rowKey mapping in mappings.dynamic");
      }
      return rowKeyField;
    } else {
      String rowKeyColumn = (String) settings.get("mappings.rowKey");
      if (rowKeyColumn == null) {
        throw new RuntimeException("must set mappings.rowKey");
      }
      Map<String, String> sinkMappings = getSinkMappings(settings);
      String rowKeyField = sinkMappings.get(rowKeyColumn);
      if (rowKeyField == null) {
        throw new RuntimeException("must set a '" + rowKeyColumn + "' mapping in mappings.sink");
      }
      return rowKeyField;
    }
  }

  /**
   * is the mapping dynamic
   */
  public static boolean isDynamicMapping(Map<String, Object> settings) {
    return settings.containsKey("types.dynamic");
  }

  /**
   * is the mapping static
   */
  public static boolean isStaticMapping(Map<String, Object> settings) {
    return settings.containsKey("types") &&
            (settings.containsKey("mappings.source") ||
                    (settings.containsKey("mappings.rowKey") &&
                            settings.containsKey("mappings.sink")));
  }

  /**
   * parse a mappingSpec, returning
   * @param mappingSpecs an ordered list of "<cassandra-col-name>:<tuple-field>" mappings
   * @return a Map of {<cassandra-col-name> => <tuple-field>} entries, with predictable iteration order the same as the
   * order of mappings in the mappingSpec
   */
  public static Map<String,String> parseMappingSpecs(List<String> mappingSpecs, String defaultTupleFieldType) {
    Map<String,String> mapping = new LinkedHashMap<String, String>();

    for(String mappingSpec : mappingSpecs) {
      String[] mappingComponents = mappingSpec.split(":");
      String cassandraColName = mappingComponents[0];
      String tupleField;
      if (mappingComponents.length>1) {
        tupleField = mappingComponents[1];
      } else {
        tupleField = defaultTupleFieldType + cassandraColName;
      }

      mapping.put(cassandraColName, tupleField);
    }

    return mapping;
  }

  public static Map<String,String> getCqlKeyMappings(Map<String, Object> settings) {
    List<String> mappingSpecs = (List<String>)settings.get("mappings.cqlKeys");
    return parseMappingSpecs(mappingSpecs, "?");
  }

  public static Map<String,String> getCqlValueMappings(Map<String, Object> settings) {
    List<String> mappingSpecs = (List<String>)settings.get("mappings.cqlValues");
    return parseMappingSpecs(mappingSpecs, "!");
  }

  public static String getSinkOutputCql(Map<String,Object> settings) {
    String cf = (String)settings.get("db.columnFamily");
    Map<String,String> valueMappings = getCqlValueMappings(settings);

    String outputCql = "UPDATE " + cf + " SET ";

    Iterator<Map.Entry<String,String>> i = valueMappings.entrySet().iterator();
    while(i.hasNext()) {
      Map.Entry<String,String> vm = i.next();
      outputCql += "\"" + vm.getKey() + "\" = ?";
      if (i.hasNext()) {
        outputCql += ", ";
      }
    }

    return outputCql;
  }
}
