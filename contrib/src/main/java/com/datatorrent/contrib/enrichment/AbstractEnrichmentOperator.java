package com.datatorrent.contrib.enrichment;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.db.cache.CacheManager;
import com.datatorrent.lib.db.cache.CacheStore;
import com.datatorrent.lib.db.cache.CacheStore.ExpiryType;
import java.io.IOException;


public abstract class AbstractEnrichmentOperator<INPUT, OUTPUT> extends BaseOperator
{
  /**
   * Keep lookup data cache for fast access.
   */
  private transient CacheManager cacheManager;

  private CacheStore primaryCache = new CacheStore();

  public transient DefaultOutputPort<OUTPUT> output = new DefaultOutputPort<OUTPUT>();

  public transient DefaultInputPort<INPUT> input = new DefaultInputPort<INPUT>()
  {
    @Override public void process(INPUT tuple)
    {
      processTuple(tuple);
    }
  };
  private CacheManager.Backup store;

  protected void processTuple(INPUT tuple) {
    Object result = cacheManager.get(getKey(tuple));
    OUTPUT out = convert(tuple, result);
    emitTuple(out);
  }

  protected abstract Object getKey(INPUT tuple);

  protected void emitTuple(OUTPUT tuple) {
    output.emit(tuple);
  }

  /* Add data from cached value to input field */
  protected abstract OUTPUT convert(INPUT in, Object cached);

  @Override public void setup(Context.OperatorContext context)
  {
    super.setup(context);

    cacheManager = new NullValuesCacheManager();

    // set expiration to one day.
    primaryCache.setEntryExpiryDurationInMillis(24 * 60 * 60 * 1000);
    primaryCache.setCacheCleanupInMillis(24 * 60 * 60 * 1000);
    primaryCache.setEntryExpiryStrategy(ExpiryType.EXPIRE_AFTER_WRITE);
    primaryCache.setMaxCacheSize(16 * 1024 * 1024);

    try {
      primaryCache.connect();
      store.connect();
      cacheManager.setPrimary(primaryCache);

      cacheManager.setBackup(store);

    } catch (IOException e) {
      throw new RuntimeException("Unable to initialize primary cache", e);
    }
  }

  public void setStore(CacheManager.Backup store) {
    this.store = store;
  }

  public CacheManager.Backup getStore() {
    return store;
  }

  public CacheStore getPrimaryCache()
  {
    return primaryCache;
  }

}
