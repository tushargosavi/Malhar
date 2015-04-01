package com.datatorrent.contrib.enrichment;

import com.datatorrent.lib.db.cache.CacheManager;

import java.util.List;
import java.util.Map;

public abstract class ReadOnlyEnrichmentBackup implements EnrichmentBackup
{

  protected transient List<String> includeFields;
  protected transient List<String> lookupFields;

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

  @Override
  public void configureFields(List<String> lookupFields, List<String> includeFields) {
    this.lookupFields = lookupFields;
    this.includeFields = includeFields;
  }
}
