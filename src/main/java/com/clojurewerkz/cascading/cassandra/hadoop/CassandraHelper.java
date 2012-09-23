package com.clojurewerkz.cascading.cassandra.hadoop;

import org.apache.cassandra.config.ConfigurationException;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;

import org.apache.cassandra.thrift.*;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.Map;
import java.util.TreeMap;
import java.util.Date;

public class CassandraHelper {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraHelper.class);
    private String cassandraHost;
    private Integer cassandraPort;
    private String keyspace;
    private String columnFamily;

    private transient Cassandra.Client _cassandraClient;
    private transient CfDef cfDef;
    private transient AbstractType keyType;
    private transient AbstractType defaultValidatorType;
    private transient Map<ByteBuffer, AbstractType> validatorsMap;

    public CassandraHelper(String host, Integer port, String keyspace, String columnFamily) {
        this.cassandraHost = host;
        this.cassandraPort = port;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
    }

    public Cassandra.Client cassandraClient() {
        try {
            if (this._cassandraClient == null)
                this._cassandraClient = createConnection(this.cassandraHost, this.cassandraPort, true);
            return this._cassandraClient;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Cassandra.Client createConnection(String host, Integer port, boolean framed) throws IOException {
        TSocket socket = new TSocket(host, port);
        TTransport trans = framed ? new TFramedTransport(socket) : socket;
        try {
            trans.open();
        } catch (TTransportException e) {
            throw new IOException("unable to connect to server", e);
        }
        return new Cassandra.Client(new TBinaryProtocol(trans));
    }


    public Map<ByteBuffer, AbstractType> makeValidatorMap(CfDef cfDef) throws IOException {
        TreeMap<ByteBuffer, AbstractType> validators = new TreeMap<ByteBuffer, AbstractType>();
        for (ColumnDef cd : getCfDef().getColumn_metadata()) {
            if (cd.getValidation_class() != null && !cd.getValidation_class().isEmpty()) {
                try {
                    validators.put(cd.name, TypeParser.parse(cd.getValidation_class()));
                } catch (ConfigurationException e) {
                    throw new IOException(e);
                }
            }
        }
        return validators;
    }

    public Map<ByteBuffer, AbstractType> getValidatorsMap() {
        try {
            if (this.validatorsMap == null)
                this.validatorsMap = this.makeValidatorMap(this.getCfDef());
            return this.validatorsMap;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private AbstractType getDefaultValidatorType() {
        if (this.defaultValidatorType == null) {
            try {
                this.defaultValidatorType = TypeParser.parse(this.getCfDef().getDefault_validation_class());
            } catch (ConfigurationException e) {
                throw new RuntimeException(e);
            }
        }
        return this.defaultValidatorType;
    }

    public AbstractType getTypeForColumn(IColumn column) {
        AbstractType type = this.getValidatorsMap().get(column.name());
        if (type == null)
            type = this.getDefaultValidatorType();
        return type;
    }

    public CfDef getCfDef() {
        if (this.cfDef == null) {
            try {
                Cassandra.Client client = this.cassandraClient();
                client.set_keyspace(this.keyspace);
                KsDef ksDef = client.describe_keyspace(this.keyspace);
                for (CfDef def : ksDef.getCf_defs()) {
                    if (this.columnFamily.equals(def.getName())) {
                        this.cfDef = def;
                        break;
                    }
                }
            } catch (TException e) {
                throw new RuntimeException(e);
            } catch (InvalidRequestException e) {
                throw new RuntimeException(e);
            } catch (NotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        return this.cfDef;
    }

    public static ByteBuffer serialize(Object obj) {
        if (obj == null) {
            return null;
        } else if (obj instanceof BigInteger) {
            LOG.debug("Serializing {} as BigInteger.", obj);
            return bigIntegerToByteBuffer((BigInteger) obj);
        } else if (obj instanceof Boolean) {
            LOG.debug("Serializing {} as Boolean.", obj);
            return booleanToByteBuffer((Boolean) obj);
        } else if (obj instanceof Date) {
            LOG.debug("Serializing {} as Date.", obj);
            return dateToByteBuffer((Date) obj);
        } else if (obj instanceof Double) {
            LOG.debug("Serializing {} as Double.", obj);
            return doubleToByteBuffer((Double) obj);
        } else if (obj instanceof BigDecimal) {
            LOG.debug("Serializing {} as Double, casted from BigDecimal.", obj);
            return doubleToByteBuffer(((BigDecimal) obj).doubleValue());
        }
        else if (obj instanceof Float) {
            LOG.debug("Serializing {} as Float.", obj);
            return floatToByteBuffer((Float) obj);
        } else if (obj instanceof Integer) {
            LOG.debug("Serializing {} as Integer.", obj);
            return intToByteBuffer((Integer) obj);
        } else if (obj instanceof Long) {
            LOG.debug("Serializing {} as Long.", obj);
            return longToByteBuffer((Long) obj);
        } else if (obj instanceof Short) {
            LOG.debug("Serializing {} as Short.", obj);
            return shortToByteBuffer((Short) obj);
        } else if (obj instanceof String) {
            LOG.debug("Serializing {} as String.", obj);
            return stringToByteBuffer((String) obj);
        }

        LOG.error("Could not serialize {}. Java reports type: {}", obj, obj.getClass().toString());

        return null;
    }

    public static ByteBuffer bigIntegerToByteBuffer(BigInteger obj) {
        return ByteBuffer.wrap(obj.toByteArray());
    }

    public static ByteBuffer booleanToByteBuffer(Boolean obj) {
        boolean bool = obj;
        byte[] b = new byte[1];
        b[0] = bool ? (byte) 1 : (byte) 0;

        return ByteBuffer.wrap(b);
    }

    public static ByteBuffer dateToByteBuffer(Date obj) {
        return longToByteBuffer(obj.getTime());
    }

    public static ByteBuffer longToByteBuffer(Long obj) {
        return ByteBuffer.allocate(8).putLong(0, obj);
    }

    public static ByteBuffer doubleToByteBuffer(Double obj) {
        return ByteBuffer.allocate(8).putDouble(0, obj);
    }

    public static ByteBuffer floatToByteBuffer(Float obj) {
        return intToByteBuffer(Float.floatToRawIntBits(obj));
    }

    public static ByteBuffer intToByteBuffer(Integer obj) {
        return ByteBuffer.allocate(4).putInt(0, obj);
    }

    public static ByteBuffer shortToByteBuffer(Short obj) {
        ByteBuffer b = ByteBuffer.allocate(2);
        b.putShort(obj);
        b.rewind();
        return b;
    }

    public static ByteBuffer stringToByteBuffer(String obj) {
        return ByteBuffer.wrap(obj.getBytes());
    }

}
