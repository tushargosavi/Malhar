/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.demos.dimensions.ads;

import java.util.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.*;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.*;

class ByteArrayInspectorFactory
{

  static HashMap<TypeInfo, ObjectInspector> cachedByteArrayObjectInspector = new HashMap<TypeInfo, ObjectInspector>();

  /**
   *
   *
   * @param options
   * @see JsonUtils
   * @param typeInfo
   * @return
   */
  public static ObjectInspector getByteArrayInspectorFromTypeInfo(
          TypeInfo typeInfo,ByteArrayStructOIOptions options)
  {
    ObjectInspector result = cachedByteArrayObjectInspector.get(typeInfo);
    if (result == null) {
      switch (typeInfo.getCategory()) {
        case PRIMITIVE: {
          PrimitiveTypeInfo pti = (PrimitiveTypeInfo)typeInfo;

          result
                  = getPrimitiveJavaObjectInspector(pti.getPrimitiveCategory());
          break;
        }
        /*case LIST: {
         ObjectInspector elementObjectInspector
         = getJsonObjectInspectorFromTypeInfo(
         ((ListTypeInfo) typeInfo).getListElementTypeInfo());
         result = ByteArrayInspectorFIactory.getByteArrayListObjectInspector(elementObjectInspector);
         break;
         }
         case MAP: {
         MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
         ObjectInspector keyObjectInspector = getJsonObjectInspectorFromTypeInfo(mapTypeInfo.getMapKeyTypeInfo());
         ObjectInspector valueObjectInspector = getJsonObjectInspectorFromTypeInfo(mapTypeInfo.getMapValueTypeInfo());
         result = ByteArrayInspectorFIactory.getJsonMapObjectInspector(keyObjectInspector,
         valueObjectInspector);
         break;
         }
         case STRUCT: {
         StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
         List<String> fieldNames = structTypeInfo.getAllStructFieldNames();
         List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
         List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(
         fieldTypeInfos.size());
         for (int i = 0; i < fieldTypeInfos.size(); i++) {
         fieldObjectInspectors.add(getJsonObjectInspectorFromTypeInfo(
         fieldTypeInfos.get(i), options));
         }
         result = JsonObjectInspectorFactory.getJsonStructObjectInspector(fieldNames,
         fieldObjectInspectors, options);
         break;
         }*/
        default: {
          result = null;
        }
      }
      cachedByteArrayObjectInspector.put(typeInfo, result);
    }
    return result;
  }

  /*
   * Caches Struct Object Inspectors
   */
  static HashMap<ArrayList<Object>, ByteArrayStructObjectInspector> cachedStandardStructObjectInspector
          = new HashMap<ArrayList<Object>, ByteArrayStructObjectInspector>();

  public static ByteArrayStructObjectInspector getByteArrayStructObjectInspector(
          List<String> structFieldNames,
          List<ObjectInspector> structFieldObjectInspectors,ByteArrayStructOIOptions options
  )
  {
    ArrayList<Object> signature = new ArrayList<Object>();
    signature.add(structFieldNames);
    signature.add(structFieldObjectInspectors);
       signature.add(options);

    ByteArrayStructObjectInspector result = cachedStandardStructObjectInspector.get(signature);
    if (result == null) {
      result = new ByteArrayStructObjectInspector(structFieldNames,
                                                  structFieldObjectInspectors,options);
      cachedStandardStructObjectInspector.put(signature, result);
    }
    return result;
  }

  /*
   * Caches the List objecvt inspectors
   */
  /* static HashMap<ArrayList<Object>, JsonListObjectInspector> cachedJsonListObjectInspector
   = new HashMap<ArrayList<Object>, JsonListObjectInspector>();

   public static JsonListObjectInspector getJsonListObjectInspector(
   ObjectInspector listElementObjectInspector) {
   ArrayList<Object> signature = new ArrayList<Object>();
   signature.add(listElementObjectInspector);
   JsonListObjectInspector result = cachedJsonListObjectInspector
   .get(signature);
   if (result == null) {
   result = new JsonListObjectInspector(listElementObjectInspector);
   cachedJsonListObjectInspector.put(signature, result);
   }
   return result;
   }*/

  /*
   * Caches Map ObjectInspectors
   */
  /* static HashMap<ArrayList<Object>, JsonMapObjectInspector> cachedJsonMapObjectInspector
   = new HashMap<ArrayList<Object>, JsonMapObjectInspector>();

   public static JsonMapObjectInspector getJsonMapObjectInspector(
   ObjectInspector mapKeyObjectInspector,
   ObjectInspector mapValueObjectInspector) {
   ArrayList<Object> signature = new ArrayList<Object>();
   signature.add(mapKeyObjectInspector);
   signature.add(mapValueObjectInspector);
   JsonMapObjectInspector result = cachedJsonMapObjectInspector
   .get(signature);
   if (result == null) {
   result = new JsonMapObjectInspector(mapKeyObjectInspector,
   mapValueObjectInspector);
   cachedJsonMapObjectInspector.put(signature, result);
   }
   return result;
   }*/
  static final Map<PrimitiveCategory, AbstractPrimitiveJavaObjectInspector> primitiveOICache
          = new EnumMap<PrimitiveCategory, AbstractPrimitiveJavaObjectInspector>(PrimitiveCategory.class);

  static {
    primitiveOICache.put(PrimitiveCategory.BYTE, new JavaStringByteObjectInspector());
    primitiveOICache.put(PrimitiveCategory.SHORT, new JavaStringShortObjectInspector());
    primitiveOICache.put(PrimitiveCategory.INT, new JavaStringIntObjectInspector());
    primitiveOICache.put(PrimitiveCategory.LONG, new JavaStringLongObjectInspector());
    primitiveOICache.put(PrimitiveCategory.FLOAT, new JavaStringFloatObjectInspector());
    primitiveOICache.put(PrimitiveCategory.DOUBLE, new JavaStringDoubleObjectInspector());
    primitiveOICache.put(PrimitiveCategory.TIMESTAMP, new JavaStringTimestampObjectInspector());
  }

  /**
   * gets the appropriate adapter wrapper around the object inspector if
   * necessary, that is, if we're dealing with numbers. The JSON parser won't
   * parse the number because it's deferred (lazy).
   *
   * @param primitiveCategory
   * @return
   */
  public static AbstractPrimitiveJavaObjectInspector getPrimitiveJavaObjectInspector(
          PrimitiveCategory primitiveCategory)
  {

    if (!primitiveOICache.containsKey(primitiveCategory)) {
      primitiveOICache.put(primitiveCategory, PrimitiveObjectInspectorFactory.
                           getPrimitiveJavaObjectInspector(primitiveCategory));
    }
    return primitiveOICache.get(primitiveCategory);
  }

}
