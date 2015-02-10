/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.demos.dimensions.ads;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Writable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class ByteArraySerDeTest
{

  public ByteArraySerDeTest()
  {
  }

  @BeforeClass
  public static void setUpClass() throws Exception
  {
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
  }

  @Before
  public void setUp() throws Exception
  {

  }

  @After
  public void tearDown()
  {
  }

  public void initialize(ByteArraySerDe instance) throws Exception
  {
    System.out.println("initialize");

    Configuration conf = null;
    Properties tbl = new Properties();
    tbl.setProperty(Constants.LIST_COLUMNS, "one,two,three,four");
    tbl.setProperty(Constants.LIST_COLUMN_TYPES, "int,int,double,timestamp");

    instance.initialize(conf, tbl);
  }


     /**
     * Test of deserialize method, but passing an array.
     */
    @Test
    public void testDeserialize() throws Exception {
        ByteArraySerDe instance = new ByteArraySerDe();
        initialize(instance);

        System.out.println("deserialize");
        Writable w = new Text("{\"one\":true,\"three\":[\"red\",\"yellow\",\"orange\"],\"two\":19.5,\"four\":\"poop\"}");

        Object result =  instance.deserialize(w);


    }

    @Test
    public void testSerializeWithMapping() throws SerDeException, Exception {
        System.out.println("testSerializeWithMapping");

        ByteArraySerDe serde = new ByteArraySerDe();

        System.out.println("serialize");
        ArrayList row = new ArrayList(5);

        List<ObjectInspector> lOi = new LinkedList<ObjectInspector>();
        List<String> fieldNames = new LinkedList<String>();

        row.add(Boolean.TRUE);
        fieldNames.add("one");
        lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(Boolean.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA));

        row.add(new Float(43.2));
        fieldNames.add("two");
        lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(Float.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA));

        row.add("value1");
        fieldNames.add("four");
        lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA));

        row.add(new Integer(7898));
        fieldNames.add("ts");
        lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(Integer.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA));

        StructObjectInspector soi = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, lOi);

        Object obj = serde.serialize(row, soi);

        assertTrue(obj instanceof Text);
        assertEquals("{\"timestamp\":7898,\"two\":43.2,\"one\":true,\"three\":[],\"four\":\"value1\"}", obj.toString());

        System.out.println("Output object " + obj.toString());
    }


     public void testSerialize() throws SerDeException, Exception, Exception {
        System.out.println("serialize");

        ByteArraySerDe instance = new ByteArraySerDe();
        initialize(instance);

        ArrayList row = new ArrayList(5);

        List<ObjectInspector> lOi = new LinkedList<ObjectInspector>();
        List<String> fieldNames = new LinkedList<String>();

        row.add("HELLO");
        fieldNames.add("atext");
        lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA));

        row.add(10);
        fieldNames.add("anumber");
        lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(Integer.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA));

        List<String> array = new LinkedList<String>();
        array.add("String1");
        array.add("String2");

        row.add(array);
        fieldNames.add("alist");
        lOi.add(ObjectInspectorFactory.getStandardListObjectInspector(
                ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA)));

        lOi.add(ObjectInspectorFactory.getStandardMapObjectInspector(
                ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA),
                ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA)));


        StructObjectInspector soi = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, lOi);

        Object result = instance.serialize(row, soi);

     //   JSONObject res = new JSONObject(result.toString());
      //  assertEquals(res.getString("atext"), row.get(0));

        //assertEquals(res.get("anumber"), row.get(1));

        // after serialization the internal contents of JSONObject are destroyed (overwritten by their string representation
        // (for map and arrays)

        System.out.println("Serialized to " + result.toString());

    }


}
