package com.datatorrent.contrib.enrichment;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MapEnrichmentOperator extends AbstractEnrichmentOperator<Map<String, Object>, Map<String, Object>>
{
  private List<String> keys;
  private List<String> includeKeys;

  public List<String> getKeys()
  {
    return keys;
  }

  public void setKeys(List<String> keys)
  {
    this.keys = keys;
  }

  public List<String> getIncludeKeys()
  {
    return includeKeys;
  }

  public void setIncludeKeys(List<String> includeKeys)
  {
    this.includeKeys = includeKeys;
  }

  @Override protected Object getKey(Map<String, Object> tuple)
  {
    ArrayList<Object> keyList = new ArrayList<Object>();
    for(String key : keys) {
      keyList.add(tuple.get(key));
    }
    return keyList;
  }

  @Override protected Map<String, Object> convert(Map<String, Object> in, Object cached)
  {
    Map<String, Object> newAttributes = (Map<String, Object>)cached;
    if (includeKeys != null && includeKeys.size() != 0) {
      newAttributes = Maps.filterKeys(newAttributes, Predicates.in(includeKeys));
    }
    in.putAll(newAttributes);
    return in;
  }
}
