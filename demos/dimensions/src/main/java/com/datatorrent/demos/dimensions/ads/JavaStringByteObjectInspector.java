/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.demos.dimensions.ads;



import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableByteObjectInspector;
import org.apache.hadoop.io.ByteWritable;

/**
 *
 * @author rcongiu
 */
public  class JavaStringByteObjectInspector
    extends AbstractPrimitiveJavaObjectInspector
        implements SettableByteObjectInspector {

    public  JavaStringByteObjectInspector() {
        super(TypeEntryShim.byteType);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        if(o == null) return null;

        if(o instanceof String) {
           return new ByteWritable(ParsePrimitiveUtils.parseByte((String)o));
        } else {
           return new ByteWritable((Byte) o);
        }
    }

    @Override
    public byte get(Object o) {
        if(o instanceof String) {
           return ParsePrimitiveUtils.parseByte((String)o);
        } else {
           return ((Byte) o);
        }
    }

    @Override
    public Object create(byte value) {
        return (value);
    }

    @Override
    public Object set(Object o, byte value) {
        return value;
    }
}