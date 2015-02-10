/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.demos.dimensions.ads;

import java.sql.Timestamp;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampObjectInspector;

/**
 * A timestamp that is stored in a String
 * @author rcongiu
 */
public class JavaStringTimestampObjectInspector extends AbstractPrimitiveJavaObjectInspector
    implements SettableTimestampObjectInspector {

    public JavaStringTimestampObjectInspector() {
        super(TypeEntryShim.timestampType);
    }


    @Override
    public Object set(Object o, byte[] bytes, int offset) {
        return create(bytes,offset);
    }

    @Override
    public Object set(Object o, Timestamp tmstmp) {
        return tmstmp.toString();
    }

    @Override
    public Object set(Object o, TimestampWritable tw) {
        return create(tw.getTimestamp());
    }

    @Override
    public Object create(byte[] bytes, int offset) {
       return new TimestampWritable(bytes, offset).toString();
    }

    @Override
    public Object create(Timestamp tmstmp) {
        return tmstmp.toString();
    }

    @Override
    public TimestampWritable getPrimitiveWritableObject(Object o) {
        if(o == null) return null;

        if(o instanceof String) {
           return new TimestampWritable(ParsePrimitiveUtils.parseTimestamp((String)o));
        } else {
          return new TimestampWritable(((Timestamp) o));
        }
    }

    @Override
    public Timestamp getPrimitiveJavaObject(Object o) {
         if(o instanceof String) {
           return ParsePrimitiveUtils.parseTimestamp((String)o);
        } else {
           return ((Timestamp) o);
        }
    }


}