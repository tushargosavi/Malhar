package com.datatorrent.contrib.enrichment;

import com.datatorrent.lib.db.cache.CacheManager;

import java.util.List;

public interface EnrichmentBackup extends CacheManager.Backup
{
  public void setLookupFields(List<String> lookupFields);
  public void setIncludeFields(List<String> includeFields);
  public boolean needRefresh();
}
