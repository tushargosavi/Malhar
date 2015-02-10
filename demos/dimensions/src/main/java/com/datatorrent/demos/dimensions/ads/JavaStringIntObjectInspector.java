/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.demos.dimensions.ads;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.io.IntWritable;

/**
 *
 * @author rcongiu
 */
public class JavaStringIntObjectInspector
    extends AbstractPrimitiveJavaObjectInspector
        implements SettableIntObjectInspector {

    public JavaStringIntObjectInspector() {
        super(TypeEntryShim.intType);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        if(o == null) return null;

        if(o instanceof String) {
           return new IntWritable(ParsePrimitiveUtils.parseInt((String)o));
        } else {
           return new IntWritable((Integer) o);
        }
    }

    @Override
    public int get(Object o) {
        if(o instanceof String) {
           return ParsePrimitiveUtils.parseInt((String)o);
        } else {
           return ((Integer) o);
        }
    }

    @Override
    public Object create(int value) {
        return value;
    }

    @Override
    public Object set(Object o, int value) {
        return value;
    }
}
