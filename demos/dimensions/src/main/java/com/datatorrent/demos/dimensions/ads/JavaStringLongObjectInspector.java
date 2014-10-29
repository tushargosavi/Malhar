/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.demos.dimensions.ads;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableLongObjectInspector;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author rcongiu
 */
public class JavaStringLongObjectInspector
        extends AbstractPrimitiveJavaObjectInspector
        implements SettableLongObjectInspector {

    public JavaStringLongObjectInspector() {
        super(TypeEntryShim.longType);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        if(o == null) return null;

        if(o instanceof String) {
           return new LongWritable(ParsePrimitiveUtils.parseLong((String)o));
        } else {
          return new LongWritable(((Long) o).longValue());
        }
    }

    @Override
    public long get(Object o) {

        if(o instanceof String) {
           return ParsePrimitiveUtils.parseLong((String)o);
        } else {
          return ((Long) o);
        }
    }

    @Override
    public Object getPrimitiveJavaObject(Object o)
    {
        return get(o);
    }

    @Override
    public Object create(long value) {
        return value;
    }

    @Override
    public Object set(Object o, long value) {
        return value;
    }
}
