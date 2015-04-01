package com.datatorrent.contrib.enrichment;

import com.datatorrent.lib.db.cache.CacheManager;
import com.datatorrent.lib.db.cache.CacheStore;

import java.util.Map;

public class EnrichmentCacheManager extends NullValuesCacheManager {

    public EnrichmentCacheManager() {
        CacheStore primaryCache = new CacheStore();
        // set expiration to one day.
        primaryCache.setEntryExpiryDurationInMillis(24 * 60 * 60 * 1000);
        primaryCache.setCacheCleanupInMillis(24 * 60 * 60 * 1000);
        primaryCache.setEntryExpiryStrategy(CacheStore.ExpiryType.EXPIRE_AFTER_WRITE);
        primaryCache.setMaxCacheSize(16 * 1024 * 1024);

        super.setPrimary(primaryCache);
    }

    @Override
    protected void refreshKeys() {
        CacheStore primary = (CacheStore) getPrimary();
        primary.reset();
        Map<Object, Object> entries = backup.loadInitialData();
        if (entries != null) {
            primary.putAll(entries);
        }
    }

}
