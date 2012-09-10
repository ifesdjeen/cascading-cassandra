package com.clojurewerkz.cascading.cassandra.hadoop;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class CassandraHelper {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraHelper.class);
    private String cassandraHost;
    private Integer cassandraPort;
    private String keyspace;
    private String columnFamily;

    private transient Cassandra.Client  _cassandraClient;
    private transient CfDef cfDef;
    private transient AbstractType keyType;
    private transient AbstractType defaultValidatorType;
    private transient Map<ByteBuffer, AbstractType> validatorsMap;

    public CassandraHelper (String host, Integer port, String keyspace, String columnFamily) {
        this.cassandraHost = host;
        this.cassandraPort = port;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
    }

    public Cassandra.Client cassandraClient()
    {
        try
        {
            if (this._cassandraClient == null)
                this._cassandraClient = createConnection(this.cassandraHost, this.cassandraPort, true);
            return this._cassandraClient;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static Cassandra.Client createConnection(String host, Integer port, boolean framed) throws IOException {
        TSocket socket = new TSocket(host, port);
        TTransport trans = framed ? new TFramedTransport(socket) : socket;
        try
        {
            trans.open();
        }
        catch (TTransportException e)
        {
            throw new IOException("unable to connect to server", e);
        }
        return new Cassandra.Client(new TBinaryProtocol(trans));
    }


    public Map<ByteBuffer, AbstractType> makeValidatorMap(CfDef cfDef) throws IOException
    {
        TreeMap<ByteBuffer, AbstractType> validators = new TreeMap<ByteBuffer, AbstractType>();
        for (ColumnDef cd : getCfDef().getColumn_metadata())
        {
            if (cd.getValidation_class() != null && !cd.getValidation_class().isEmpty())
            {
                try
                {
                    validators.put(cd.name, TypeParser.parse(cd.getValidation_class()));
                }
                catch (ConfigurationException e)
                {
                    throw new IOException(e);
                }
            }
        }
        return validators;
    }

    public Map<ByteBuffer, AbstractType> getValidatorsMap()
    {
        try
        {
            if (this.validatorsMap == null)
                this.validatorsMap = this.makeValidatorMap(this.getCfDef());
            return this.validatorsMap;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private AbstractType getDefaultValidatorType()
    {
        if (this.defaultValidatorType == null)
        {
            try
            {
                this.defaultValidatorType = TypeParser.parse(this.getCfDef().getDefault_validation_class());
            }
            catch (ConfigurationException e)
            {
                throw new RuntimeException(e);
            }
        }
        return this.defaultValidatorType;
    }

    public AbstractType getTypeForColumn(IColumn column)
    {
        AbstractType type = this.getValidatorsMap().get(column.name());
        if (type == null)
            type = this.getDefaultValidatorType();
        return type;
    }

    public CfDef getCfDef()
    {
        if (this.cfDef == null)
        {
            try
            {
                Cassandra.Client client = this.cassandraClient();
                client.set_keyspace(this.keyspace);
                KsDef ksDef = client.describe_keyspace(this.keyspace);
                for (CfDef def : ksDef.getCf_defs())
                {
                    if (this.columnFamily.equals(def.getName()))
                    {
                        this.cfDef = def;
                        break;
                    }
                }
            }
            catch (TException e)
            {
                throw new RuntimeException(e);
            }
            catch (InvalidRequestException e)
            {
                throw new RuntimeException(e);
            }
            catch (NotFoundException e)
            {
                throw new RuntimeException(e);
            }
        }
        return this.cfDef;
    }
}
