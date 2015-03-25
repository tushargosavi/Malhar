package com.datatorrent.contrib.enrichment;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Map;

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
    Map<String, Object> newAttributes = (Map<String, Object>)cached;
    if (includeFieldsStr != null && includeFieldsStr.length() != 0) {
      newAttributes = Maps.filterKeys(newAttributes, Predicates.in(includeFields));
    }
    in.putAll(newAttributes);
    return in;
  }
}
