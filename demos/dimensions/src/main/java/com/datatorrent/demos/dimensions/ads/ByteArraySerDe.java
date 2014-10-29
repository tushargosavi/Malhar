/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.demos.dimensions.ads;

import java.util.*;
import kafka.log.Log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;

public class ByteArraySerDe implements SerDe
{
  private static final Logger LOG = LoggerFactory.getLogger(ByteArraySerDe.class);
  ByteArrayStructOIOptions options;
  List<String> columnNames;
  List<TypeInfo> columnTypes;
  StructTypeInfo rowTypeInfo;
  ObjectInspector rowObjectInspector;
  long deserializedDataSize;
  long serializedDataSize;
  private boolean lastOperationSerialize;
  private SerDeStats stats;
  private List<Object> row = new ArrayList<Object>();

  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException
  {
    LOG.debug("Initializing SerDe");
    stats = new SerDeStats();
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
    rowObjectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
    options
            = new ByteArrayStructOIOptions(getMappings(tbl));
  }

  @Override
  public Object deserialize(Writable w) throws SerDeException
  {
    Map<?, ?> root = null;
    row.clear();
    try {
      ObjectMapper mapper = new ObjectMapper();
      // This is really a Map<String, Object>. For more information about how
      // Jackson parses JSON in this example, see
      // http://wiki.fasterxml.com/JacksonDataBinding
      root = mapper.readValue(w.toString(), Map.class);
    }
    catch (Exception e) {
      throw new SerDeException(e);
    }

    // Lowercase the keys as expected by hive
    Map<String, Object> lowerRoot = new HashMap();
    for (Map.Entry entry: root.entrySet()) {
      lowerRoot.put(((String)entry.getKey()).toLowerCase(), entry.getValue());
    }
    root = lowerRoot;
    Object value = null;
    for (String fieldName: rowTypeInfo.getAllStructFieldNames()) {
      try {
        TypeInfo fieldTypeInfo = rowTypeInfo.getStructFieldTypeInfo(fieldName);
        value = parseField(root.get(fieldName), fieldTypeInfo);
        LOG.info("value is " + value);

      }
      catch (Exception e) {
        value = null;
        LOG.info("Error parsing empty row. This should never happen." + e.getMessage());

      }
      row.add(value);
    }

    /*Text rowText = (Text)w;
     String txt = rowText.toString().trim();
     LOG.info("Text is" + txt);
     deserializedDataSize = rowText.getBytes().length;
     // Try parsing row into AdInfo object
     AdInfo adInfoObj = null;
     // if (txt.startsWith("{")) {
     try {
     adInfoObj = new AdInfo();
     LOG.info(adInfoObj.toString());
     }
     catch (Exception e) {
     try {
     adInfoObj = new AdInfo.AdInfoAggregateEvent(Integer.valueOf("{}"));
     }
     catch (Exception ex) {
     LOG.info("Error parsing empty row. This should never happen." + ex.getMessage());
     }
     }
     // }*/
    return row;
  }

  private Object parseField(Object field, TypeInfo fieldTypeInfo)
  {
    switch (fieldTypeInfo.getCategory()) {
      case PRIMITIVE:
      // Jackson will return the right thing in this case, so just return
        // the object
        if (field instanceof String) {
          field = field.toString().replaceAll("\n", "\\\\n");
        }
        return field;
      default:
        return null;
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException
  {
    return rowObjectInspector;
  }

  @Override
  public SerDeStats getSerDeStats()
  {
    if (lastOperationSerialize) {
      stats.setRawDataSize(serializedDataSize);
    }
    else {
      stats.setRawDataSize(deserializedDataSize);
    }
    return stats;
  }

  public static final String PFX = "mapping.";

  /**
   * Builds mappings between hive columns and json attributes
   *
   * @param tbl
   * @return
   */
  private Map<String, String> getMappings(Properties tbl)
  {
    int n = PFX.length();
    Map<String, String> mps = new HashMap<String, String>();

    for (Object o: tbl.keySet()) {
      if (!(o instanceof String)) {
        continue;
      }
      String s = (String)o;

      if (s.startsWith(PFX)) {
        mps.put(s.substring(n), tbl.getProperty(s).toLowerCase());
      }
    }
    return mps;
  }

  @Override
  public Class<? extends Writable> getSerializedClass()
  {
    return Text.class;
  }

  //@Override
  /*public Writable serialize(Object obj, ObjectInspector objInspector)
   throws SerDeException
   {
   // make sure it is a struct record
   if (objInspector.getCategory() != Category.STRUCT) {
   throw new SerDeException(getClass().toString()
   + " can only serialize struct types, but we got: "
   + objInspector.getTypeName());
   }

   JSONObject serializer =
   serializeStruct( obj, (StructObjectInspector) objInspector, columnNames);

   Text t = new Text(serializer.toString());
   serializedDataSize = t.getBytes().length;
   return t;
   }*/
  @Override
  public Writable serialize(Object obj, ObjectInspector oi)
          throws SerDeException
  {
    LOG.info(obj.getClass().toString());
    Object deparsedObj = deparseRow(obj, oi);
    ObjectMapper mapper = new ObjectMapper();
    try {
      // Let Jackson do the work of serializing the object
      return new Text(mapper.writeValueAsString(deparsedObj));
    }
    catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  private Object deparseObject(Object obj, ObjectInspector oi)
  {
    switch (oi.getCategory()) {

      case PRIMITIVE:
        return deparsePrimitive(obj, (PrimitiveObjectInspector)oi);

      default:
        return null;
    }
  }

  private Object deparsePrimitive(Object obj, PrimitiveObjectInspector primOI)
  {
    return primOI.getPrimitiveJavaObject(obj);
  }

  private Object deparseRow(Object obj, ObjectInspector structOI)
  {
    return deparseStruct(obj, (StructObjectInspector)structOI, true);
  }

  private Object deparseStruct(Object obj,
          StructObjectInspector structOI,
          boolean isRow)
  {
    Map<Object, Object> struct = new HashMap<Object, Object>();
    List<? extends StructField> fields = structOI.getAllStructFieldRefs();
    for (int i = 0; i < fields.size(); i++) {
      StructField field = fields.get(i);
      // The top-level row object is treated slightly differently from other
      // structs, because the field names for the row do not correctly reflect
      // the Hive column names. For lower-level structs, we can get the field
      // name from the associated StructField object.
      String fieldName = isRow ? columnNames.get(i) : field.getFieldName();
      ObjectInspector fieldOI = field.getFieldObjectInspector();
      Object fieldObj = structOI.getStructFieldData(obj, field);
      struct.put(fieldName, deparseObject(fieldObj, fieldOI));
    }
    return struct;
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
                    // we want to serialize columns with their proper HIVE name,
          // not the _col2 kind of name usually generated upstream
          result.put(
                  getSerializedFieldName(columnNames, i, sf),
                  serializeField(
                          data,
                          sf.getFieldObjectInspector()));

        }
        catch (JSONException ex) {
          LOG.warn("Problem serializing", ex);
          throw new RuntimeException(ex);
        }
      }
    }
    return result;

  }

  private String getSerializedFieldName(List<String> columnNames, int pos, StructField sf)
  {
    String n = (columnNames == null ? sf.getFieldName() : columnNames.get(pos));

    if (options.getMappings().containsKey(n)) {
      return options.getMappings().get(n);
    }
    else {
      return n;
    }
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
      /* case MAP:
       result = serializeMap(obj, (MapObjectInspector)oi);
       break;
       case LIST:
       result = serializeArray(obj, (ListObjectInspector)oi);
       break;*/
      case STRUCT:
        result = serializeStruct(obj, (StructObjectInspector)oi, null);
        break;
    }
    return result;
  }

}
