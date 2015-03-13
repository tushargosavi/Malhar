/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.Slice;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregateEvent;
import com.google.common.cache.*;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * AdsDimension Store Operator with cache in front of HDHT.
 *
 * @displayName Dimensional Store
 * @category Store
 * @tags storage, hdfs, dimensions, hdht
 */
public class AdsDimensionStoreOperatorWithCache extends AdsDimensionStoreOperator implements RemovalListener<AdInfoAggregateEvent, AdInfoAggregateEvent>
{
  private static final Logger LOG = LoggerFactory.getLogger(AdsDimensionStoreOperatorWithCache.class);

  // in-memory aggregation before hitting WAL
  transient LoadingCache<AdInfoAggregateEvent, AdInfoAggregateEvent> aggrCache = null;

  // TODO: should be aggregation interval count
  private int maxCacheSize = 1024 * 1024;

  private boolean useCache = true;

  public int getMaxCacheSize()
  {
    return maxCacheSize;
  }

  public void setMaxCacheSize(int maxCacheSize)
  {
    this.maxCacheSize = maxCacheSize;
  }

  public boolean isUseCache()
  {
    return useCache;
  }

  public void setUseCache(boolean useCache)
  {
    this.useCache = useCache;
  }

  @Override
  protected void processEvent(AdInfoAggregateEvent event) throws IOException
  {
    if (useCache)
      processEventWithCache(event);
    else
      processEventWithoutCache(event);
  }

  protected void processEventWithCache(AdInfoAggregateEvent event)
  {
    AdInfoAggregateEvent old = null;
    try {
      old = aggrCache.get(event);
      c.hdhtHits++;
    } catch (ExecutionException e) {
      c.hdhtMisses++;
      old = null;
    }

    if (old != null)
      aggregator.aggregate(old, event);
    else
      aggrCache.put(event, event);
  }

  protected void processEventWithoutCache(AdInfoAggregateEvent event) throws IOException
  {
    AdInfoAggregateEvent old = getEventFromHDS(event);

    if (old != null)
      aggregator.aggregate(old, event);
    else {
      old = event;
      c.hdhtMisses++;
    }
    super.put(getBucketKey(old), new Slice(getKey(old)), getValue(old));
  }

  @Override public void setup(Context.OperatorContext arg0)
  {
    this.context = arg0;
    super.setup(arg0);
    aggrCache = CacheBuilder.newBuilder()
        .maximumSize(maxCacheSize)
        .removalListener(this)
        .expireAfterWrite(5, TimeUnit.MINUTES)
        .build(new CacheLoader<AdInfoAggregateEvent, AdInfoAggregateEvent>()
        {
          @Override public AdInfoAggregateEvent load(AdInfoAggregateEvent adInfoAggregateEvent) throws Exception
          {
            AdInfoAggregateEvent ae = getEventFromHDS(adInfoAggregateEvent);
            /* can't add null into cache */
            if (ae == null)
              throw new Exception("Null value");

            return ae;
          }
        });
  }

  CacheCounters c = new CacheCounters();

  @Override public void endWindow()
  {
    super.endWindow();
    c.hdhtCounters = bucketStats;
    CacheStats cacheStats = aggrCache.stats();
    c.hit = cacheStats.hitCount();
    c.miss = cacheStats.missCount();
    context.setCounters(c);
    LOG.info("number of keys in aggregate cache {} maxsize {}", aggrCache.size(), maxCacheSize);
    LOG.info("cache stats {}", c);
  }

  @Override public void onRemoval(RemovalNotification<AdInfoAggregateEvent, AdInfoAggregateEvent> removalNotification)
  {
    AdInfoAggregateEvent key = removalNotification.getKey();
    AdInfoAggregateEvent val = removalNotification.getValue();

    try {
      super.put(getBucketKey(key), new Slice(getKey(key)), getValue(val));
    } catch (IOException ex) {
      throw new RuntimeException("Unable to add key to HDS");
    }
  }

  private AdInfoAggregateEvent getEventFromHDS(AdInfoAggregateEvent event) throws IOException
  {
    byte[] key = getKey(event);
    if (key == null)
      return null;
    Slice keySlice = new Slice(key, 0, key.length);
    byte[] val = getUncommitted(getBucketKey(event), keySlice);

    if (val == null)
      val = get(getBucketKey(event), keySlice);
    else
      c.hdhtMemHits++;

    if (val == null)
      return null;

    AdInfoAggregateEvent old = bytesToAggregate(keySlice, val);
    c.hdhtHits++;
    return old;
  }

  @JsonSerialize
  public static class CacheCounters implements Serializable {
    public long hit;
    public long miss;
    public long hdhtHits;
    public long hdhtMisses;
    public Object hdhtCounters;
    public long hdhtMemHits;

    public CacheCounters() { }

    public CacheCounters(long h, long m, Object o) {
      hit = h;
      miss = m;
      hdhtCounters = o;
      hdhtHits = 0;
      hdhtMemHits = 0;
      hdhtMisses = 0;
    }

    @Override public String toString()
    {
      return "CacheCounters{" +
          "hit=" + hit +
          ", miss=" + miss +
          ", hdht=" + hdhtCounters +
          '}';
    }
  }

  public static class StatAggregator implements Serializable, Context.CountersAggregator
  {
    private static final long serialVersionUID = 201412091454L;

    BucketIOStatAggregator aggregator = new BucketIOStatAggregator();

    @Override public Object aggregate(Collection<?> countersList)
    {
      List lst = new ArrayList();
      long hits = 0;
      long misses = 0;
      for (Object o : countersList) {
        CacheCounters c = (CacheCounters)o;
        System.out.println("Adding counters " + c.hdhtCounters);
        lst.add(c.hdhtCounters);
        hits += c.hit;
        misses += c.miss;
      }
      Object aggrStats = aggregator.aggregate(lst);
      System.out.printf("Final hdht counters " + aggrStats);
      return new CacheCounters(hits, misses, aggrStats);
    }
  }

  public static class CacheStatsAggregator implements Serializable, Context.CountersAggregator {
    @Override public Object aggregate(Collection<?> countersList)
    {
      long hits = 0;
      long misses = 0;
      for (Object o : countersList) {
        System.out.println("processing counter " + o);
        CacheCounters c = (CacheCounters)o;
        hits += c.hit;
        misses += c.miss;
      }
      return new CacheCounters(hits, misses, null);
    }
  }
}
