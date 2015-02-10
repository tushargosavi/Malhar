/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.demos.dimensions.ads;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;

import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableShortObjectInspector;

/**
 *
 * @author rcongiu
 */
public class JavaStringShortObjectInspector
        extends AbstractPrimitiveJavaObjectInspector
        implements SettableShortObjectInspector {

    public JavaStringShortObjectInspector() {
        super(TypeEntryShim.shortType);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        if(o == null) return null;

        if(o instanceof String) {
          return new ShortWritable(ParsePrimitiveUtils.parseShort((String)o));
        } else {
          return new ShortWritable((Short) o);
        }
    }

    @Override
    public short get(Object o) {

        if(o instanceof String) {
           return ParsePrimitiveUtils.parseShort((String)o);
        } else {
          return ((Short) o);
        }
    }

    @Override
    public Object create(short value) {
        return value;
    }

    @Override
    public Object set(Object o, short value) {
        return value;
    }
}
