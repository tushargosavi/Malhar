/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.demos.dimensions.ads;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDoubleObjectInspector;
import org.apache.hadoop.io.DoubleWritable;

/**
 *
 * @author rcongiu
 */
public class JavaStringDoubleObjectInspector extends AbstractPrimitiveJavaObjectInspector
        implements SettableDoubleObjectInspector {

    public JavaStringDoubleObjectInspector() {
        super(TypeEntryShim.doubleType);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        if(o == null) return null;

        if(o instanceof String) {
           return new DoubleWritable(Double.parseDouble((String)o));
        } else {
          return new DoubleWritable(((Double) o));
        }
    }

    @Override
    public double get(Object o) {

        if(o instanceof String) {
           return Double.parseDouble((String)o);
        } else {
          return (((Double) o));
        }
    }

    @Override
    public Object create(double value) {
        return value;
    }

    @Override
    public Object set(Object o, double value) {
        return value;
    }

}
