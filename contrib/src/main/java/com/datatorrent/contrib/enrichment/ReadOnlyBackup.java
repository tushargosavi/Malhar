package com.datatorrent.contrib.enrichment;

import com.datatorrent.lib.db.cache.CacheManager;

import java.util.Map;

public abstract class ReadOnlyBackup implements CacheManager.Backup
{

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

}
