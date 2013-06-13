package com.ifesdjeen.cascading.cassandra.hadoop;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;

import java.nio.ByteBuffer;
import java.util.*;

public class SerializerHelper {

    public static Object deserialize(ByteBuffer bb, String type) throws SyntaxException, ConfigurationException {

        AbstractType t = inferType(type);

        if (t instanceof CompositeType) {
            CompositeType ct = (CompositeType)t;
            List<AbstractType<?>> componentTypes = ct.types;
            List<AbstractCompositeType.CompositeComponent> components = ct.deconstruct(bb);

            ArrayList objs = new ArrayList();
            for(int i=0 ; i < componentTypes.size() ; i++) {
                AbstractType componentType = componentTypes.get(i);
                Object obj = componentType.compose(components.get(i).value);
                objs.add(obj);
            }

            return objs;
        } else {
            return t.compose(bb);
        }
    }

  public static AbstractType inferType(String t) throws SyntaxException, ConfigurationException {
    return org.apache.cassandra.db.marshal.TypeParser.parse(t);
  }
}
