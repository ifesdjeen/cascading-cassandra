package com.clojurewerkz.cascading.cassandra.db;

import cascading.tuple.Tuple;
import com.clojurewerkz.cascading.cassandra.db.CassandraWritable;

import java.io.DataOutput;
import java.util.HashMap;

public class TupleRecord implements CassandraWritable {

    private Tuple tuple;

    public TupleRecord() {
    }

    public TupleRecord( Tuple tuple ) {
        this.tuple = tuple;
    }

    public void setTuple( Tuple tuple ) {
        this.tuple = tuple;
    }

    public Tuple getTuple() {
        return tuple;
    }

    public void write( DataOutput out ) {

    }

    public void readFields( HashMap resultSet )  {
        tuple = new Tuple();


        //for( int i = 0; i < resultSet.getMetaData().getColumnCount(); i++ )
        //    tuple.add( (Comparable) resultSet.getObject( i + 1 ) );
    }

}
