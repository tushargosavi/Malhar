package com.datatorrent.contrib.enrichment;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Map;

/**
 * Enrichment
 * <p>
 * This class takes a HashMap tuple as input and extract the value of the lookupKey configured
 * for this operator. It then does a lookup in file to find matching entry and all key-value pairs
 * specified in the file is added to original tuple.
 *
 * The file contains data in json format, one entry per line. during setup entire file is read and
 * kept in memory for quick lookup.
 *
 * Example
 * If file contains following lines, and operator is configured with lookup key "productId"
 * { "productId": 1, "productCategory": 3 }
 * { "productId": 4, "productCategory": 10 }
 * { "productId": 3, "productCategory": 1 }
 *
 * And input tuple is
 * { amount=10.0, channelId=4, productId=3 }
 *
 * The tuple is modified as below before operator emits it on output port.
 * { amount=10.0, channelId=4, productId=3, productCategory=1 }
 * </p>
 *
 * @displayName MapEnrichment
 * @category Database
 * @tags enrichment, lookup
 *
 * @since 2.0.0
 */
public class MapEnrichmentOperator extends AbstractEnrichmentOperator<Map<String, Object>, Map<String, Object>>
{
  @Override protected Object getKey(Map<String, Object> tuple)
  {
    ArrayList<Object> keyList = new ArrayList<Object>();
    for(String key : lookupFields) {
      keyList.add(tuple.get(key));
    }
    return keyList;
  }

  @Override protected Map<String, Object> convert(Map<String, Object> in, Object cached)
  {
    if (cached == null)
      return in;

    ArrayList<Object> newAttributes = (ArrayList<Object>)cached;
    if(newAttributes != null) {
      for (int i = 0; i < includeFields.size(); i++) {
        in.put(includeFields.get(i), newAttributes.get(i));
      }
    }
    return in;
  }
}
