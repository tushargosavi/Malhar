/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.demos.dimensions.ads;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableFloatObjectInspector;
import org.apache.hadoop.io.FloatWritable;

/**
 *
 * @author rcongiu
 */
public class JavaStringFloatObjectInspector extends AbstractPrimitiveJavaObjectInspector
        implements SettableFloatObjectInspector {

    public JavaStringFloatObjectInspector() {
        super(TypeEntryShim.floatType);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        if(o == null) return null;

        if(o instanceof String) {
          return new FloatWritable(Float.parseFloat((String)o));
        } else {
          return new FloatWritable((Float) o);
        }
    }

    @Override
    public float get(Object o) {
        if(o instanceof String) {
          return Float.parseFloat((String)o);
        } else {
          return ((Float) o);
        }
    }

    @Override
    public Object create(float value) {
        return value;
    }

    @Override
    public Object set(Object o, float value) {
        return value;
    }

}
