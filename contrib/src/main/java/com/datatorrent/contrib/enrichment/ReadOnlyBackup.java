package com.datatorrent.contrib.enrichment;

import com.datatorrent.lib.db.cache.CacheManager;

import java.util.List;
import java.util.Map;

public abstract class ReadOnlyBackup implements EnrichmentBackup
{

  protected List<String> includeFields;
  protected List<String> lookupFields;

  @Override public void put(Object key, Object value)
  {
    throw new RuntimeException("Not supported operation");
  }

  @Override public void putAll(Map<Object, Object> m)
  {
    throw new RuntimeException("Not supported operation");
  }

  @Override public void remove(Object key)
  {
    throw new RuntimeException("Not supported operation");
  }


  @Override public void setLookupFields(List<String> lookupFields)
  {
    this.lookupFields = lookupFields;
  }

  @Override public void setIncludeFields(List<String> includeFields)
  {
    this.includeFields = includeFields;
  }
}
