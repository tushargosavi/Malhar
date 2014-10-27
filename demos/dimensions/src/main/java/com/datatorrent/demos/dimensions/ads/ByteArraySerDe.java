/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregateEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde.Constants;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.*;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.codehaus.jettison.json.JSONObject;

public class ByteArraySerDe implements SerDe
{
  private static final Logger LOG = LoggerFactory.getLogger(ByteArraySerDe.class);

  List<String> columnNames;
  List<TypeInfo> columnTypes;
  StructTypeInfo rowTypeInfo;
  StructObjectInspector rowObjectInspector;

  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException
  {

    LOG.debug("Initializing SerDe");
    // Get column names and types
    String columnNameProperty = tbl.getProperty(Constants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(Constants.LIST_COLUMN_TYPES);

    LOG.debug("columns " + columnNameProperty + " types " + columnTypeProperty);

    // all table column names
    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<String>();
    }
    else {
      columnNames = Arrays.asList(columnNameProperty.split(","));
    }

    // all column types
    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    }
    else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }
    assert (columnNames.size() == columnTypes.size());
    // Create row related objects
    rowTypeInfo = (StructTypeInfo)TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
   // rowObjectInspector = (StructObjectInspector)PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(rowTypeInfo);

  }

  /* public static AbstractPrimitiveJavaObjectInspector getPrimitiveJavaObjectInspector(PrimitiveCategory primitiveCategory) {
   return  JavaByteObjectInspector.;
   }*/
  @Override
  public Object deserialize(Writable w) throws SerDeException
  {
    Text rowText = (Text)w;

    // Try parsing row into ad info object
    AdInfo.AdInfoAggregateEvent adObj = null;
    try {
      adObj = new AdInfoAggregateEvent(rowText.toString())
      {

        public AdInfo.AdInfoAggregateEvent put(String key, Object value)
                throws Exception
        {
          return super.put(key.toLowerCase(), value);
        }

      };
    }
    catch (Exception e) {
      // If row is not an object, make the whole row NULL
      LOG.error("Row is not a valid Object - Exception: "
              + e.getMessage());
      throw new SerDeException(e);
    }
    return adObj;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public SerDeStats getSerDeStats()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Class<? extends Writable> getSerializedClass()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
          throws SerDeException
  {
    // make sure it is a struct record
    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
              + " can only serialize struct types, but we got: "
              + objInspector.getTypeName());
    }

    JSONObject serializer
            = serializeStruct(obj, (StructObjectInspector)objInspector, columnNames);

    Text t = new Text(serializer.toString());

    return t;
  }

  private JSONObject serializeStruct(Object obj,
          StructObjectInspector soi, List<String> columnNames)
  {
    // do nothing for null struct
    if (null == obj) {
      return null;
    }

    JSONObject result = new JSONObject();

    List<? extends StructField> fields = soi.getAllStructFieldRefs();

    for (int i = 0; i < fields.size(); i++) {
      StructField sf = fields.get(i);
      Object data = soi.getStructFieldData(obj, sf);

      if (null != data) {
        try {
          result.put((columnNames == null ? sf.getFieldName() : columnNames.get(i)),
                     serializeField(
                             data,
                             sf.getFieldObjectInspector()));

        }
        catch (Exception ex) {
          LOG.warn("Problem serializing", ex);
          throw new RuntimeException(ex);
        }
      }
    }
    return result;

  }

  Object serializeField(Object obj,
          ObjectInspector oi)
  {
    if (obj == null) {
      return null;
    }

    Object result = null;
    switch (oi.getCategory()) {
      case PRIMITIVE:
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector)oi;
        switch (poi.getPrimitiveCategory()) {
          case VOID:
            result = null;
            break;
          case BOOLEAN:
            result = (((BooleanObjectInspector)poi).get(obj)
                      ? Boolean.TRUE
                      : Boolean.FALSE);
            break;
          case BYTE:
            result = (((ShortObjectInspector)poi).get(obj));
            break;
          case DOUBLE:
            result = (((DoubleObjectInspector)poi).get(obj));
            break;
          case FLOAT:
            result = (((FloatObjectInspector)poi).get(obj));
            break;
          case INT:
            result = (((IntObjectInspector)poi).get(obj));
            break;
          case LONG:
            result = (((LongObjectInspector)poi).get(obj));
            break;
          case SHORT:
            result = (((ShortObjectInspector)poi).get(obj));
            break;
          case STRING:
            result = (((StringObjectInspector)poi).getPrimitiveJavaObject(obj));
            break;
          case UNKNOWN:
            throw new RuntimeException("Unknown primitive");
        }
        break;
      case MAP:
        result = serializeMap(obj, (MapObjectInspector)oi);
        break;
      case LIST:
        result = serializeArray(obj, (ListObjectInspector)oi);
        break;
      case STRUCT:
        result = serializeStruct(obj, (StructObjectInspector)oi, null);
        break;
    }
    return result;
  }

}
