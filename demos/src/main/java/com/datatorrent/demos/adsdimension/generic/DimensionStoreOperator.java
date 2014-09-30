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
package com.datatorrent.demos.adsdimension.generic;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hds.AbstractSinglePortHDSWriter;
import com.datatorrent.contrib.hds.HDS;
import com.datatorrent.demos.adsdimension.HDSQueryOperator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class DimensionStoreOperator extends AbstractSinglePortHDSWriter<MapAggregate>
{
  protected final SortedMap<Long, Map<MapAggregate, MapAggregate>> cache = Maps.newTreeMap();

  private int maxCacheSize = 5;
  private MapAggregator aggregator;
  private transient EventSchema eventSchema;
  protected transient GenericEventSerializer serializer;

  public int getMaxCacheSize()
  {
    return maxCacheSize;
  }

  public void setMaxCacheSize(int maxCacheSize)
  {
    this.maxCacheSize = maxCacheSize;
  }

  public MapAggregator getAggregator()
  {
    return aggregator;
  }

  public void setAggregator(MapAggregator aggregator)
  {
    this.aggregator = aggregator;
  }

  public final transient DefaultOutputPort<HDSRangeQueryResult> queryResult = new DefaultOutputPort<HDSRangeQueryResult>();

  // Set default schema to ADS
  private String eventSchemaJSON = EventSchema.DEFAULT_SCHEMA_ADS;

  public String getEventSchemaJSON()
  {
    return eventSchemaJSON;
  }

  public void setEventSchemaJSON(String eventSchemaJSON)
  {
    this.eventSchemaJSON = eventSchemaJSON;
    setAggregator(new MapAggregator(getEventSchema()));
  }

  public EventSchema getEventSchema() {
    if (eventSchema == null) {
      try {
        eventSchema = EventSchema.createFromJSON(eventSchemaJSON);
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to parse JSON input: " + eventSchemaJSON, e);
      }
    }
    return eventSchema;
  }


  public transient final DefaultInputPort<String> query = new DefaultInputPort<String>()
  {
    @Override
    public void process(String s)
    {
      try {
        LOG.debug("registering query {}", s);
        registerQuery(s);
      } catch(Exception ex) {
        LOG.error("Unable to register query {}", s);
      }
    }
  };

  static class HDSRangeQuery
  {
    public String id;
    public int windowCountdown;
    public MapAggregate prototype;
    public long startTime;
    public long endTime;
    public TimeUnit intervalTimeUnit = TimeUnit.MINUTES;
    private transient LinkedHashSet<HDSQuery> points = Sets.newLinkedHashSet();

    @Override
    public String toString()
    {
      return "HDSRangeQuery{" +
          "id='" + id + '\'' +
          ", windowCountdown=" + windowCountdown +
          ", startTime=" + startTime +
          ", endTime=" + endTime +
          '}';
    }
  }

  /**
   * Parameters for registration of query.
   */
  static class QueryParameters
  {
    public String id;
    public int numResults;
    public Map<String, String> keys;
    public long startTime;
    public long endTime;
  }

  static public class HDSRangeQueryResult
  {
    public String id;
    public long countDown;
    public List<Map<String, Object>> data;
  }

  @VisibleForTesting
  protected transient final Map<String, HDSRangeQuery> rangeQueries = Maps.newConcurrentMap();
  private transient ObjectMapper mapper = null;
  private long defaultTimeWindow = TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES);

  public long getDefaultTimeWindow()
  {
    return defaultTimeWindow;
  }

  public void setDefaultTimeWindow(long defaultTimeWindow)
  {
    this.defaultTimeWindow = defaultTimeWindow;
  }

  public void registerQuery(String queryString) throws Exception
  {
    if (mapper == null) {
      mapper = new ObjectMapper();
      mapper.configure(org.codehaus.jackson.map.DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    QueryParameters queryParams = mapper.readValue(queryString, QueryParameters.class);

    if (queryParams.id == null) {
      return;
    }

    @SuppressWarnings("unchecked")
    MapAggregate ae = new MapAggregate(getEventSchema(), mapper.convertValue(queryParams.keys, Map.class));

    long bucketKey = getBucketKey(ae);
    if (!(super.partitions == null || super.partitions.contains((int)bucketKey))) {
      //LOG.debug("Ignoring query for bucket {} when this partition serves {}", bucketKey, super.partitions);
      return;
    }

    HDSRangeQuery query = new HDSRangeQuery();
    query.id = queryParams.id;
    query.prototype = ae;
    query.windowCountdown = 30;

    query.endTime = queryParams.endTime;
    if (queryParams.endTime == 0) {
      // If endTime is not specified, then use current system time as end time.
      query.endTime = System.currentTimeMillis();
    }
    query.endTime = TimeUnit.MILLISECONDS.convert(query.intervalTimeUnit.convert(query.endTime, TimeUnit.MILLISECONDS), query.intervalTimeUnit);

    query.startTime = queryParams.startTime;
    if (queryParams.startTime == 0) {
      // If start time is not specified, return data for configured number of intervals (defaultTimeWindow)
      query.startTime = query.endTime - defaultTimeWindow;
    }
    query.startTime = TimeUnit.MILLISECONDS.convert(query.intervalTimeUnit.convert(query.startTime, TimeUnit.MILLISECONDS), query.intervalTimeUnit);

    // set query for each point in series
    query.prototype.setTimestamp(query.startTime);
    while (query.prototype.getTimestamp() <= query.endTime) {
      Slice key = HDS.SliceExt.toSlice(codec.getKeyBytes(query.prototype));
      HDSQuery q = super.queries.get(key);
      if (q == null) {
        q = new HDSQuery();
        q.bucketKey = bucketKey;
        q.key = key;
        super.addQuery(q);
      } else {
        // TODO: find out why we got null in first place
        if (q.result == null) {
          LOG.debug("Forcing refresh for {}", q);
          q.processed = false;
        }
      }
      q.keepAliveCount = query.windowCountdown;
      query.points.add(q);
      query.prototype.setTimestamp(query.prototype.getTimestamp() + query.intervalTimeUnit.toMillis(1));
    }
    LOG.debug("Queries: {}", query.points);
    rangeQueries.put(query.id, query);
  }

  @Override
  protected Class<? extends HDSCodec<MapAggregate>> getCodecClass()
  {
    return MapAggregateCodec.class;
  }

  @Override
  protected void processEvent(MapAggregate aggr) throws IOException
  {
    // aggregate in-memory, flush to store in endWindow
    Map<MapAggregate, MapAggregate> valMap = cache.get(aggr.getTimestamp());
    if (valMap == null) {
      valMap = new HashMap<MapAggregate, MapAggregate>();
      valMap.put(aggr, aggr);
      cache.put(aggr.getTimestamp(), valMap);
    } else {
      MapAggregate val = valMap.get(aggr);
      if (val == null) {
        valMap.put(aggr, aggr);
        return;
      } else {
        aggregator.aggregate(val, aggr);
      }
    }
  }

  @Override
  public void endWindow()
  {
    super.endWindow();

    // flush final aggregates to HDS
    int expiredEntries = cache.size() - maxCacheSize;
    while(expiredEntries-- > 0){

      Map<MapAggregate, MapAggregate> vals = cache.remove(cache.firstKey());
      for (Map.Entry<MapAggregate, MapAggregate> en : vals.entrySet()) {
        MapAggregate ai = en.getValue();
        try {
          super.processEvent(ai);
        } catch (IOException e) {
          LOG.warn("Error putting the value", e);
        }
      }
    }

    // process queries
    Iterator<Map.Entry<String, HDSRangeQuery>> it = this.rangeQueries.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, HDSRangeQuery> e = it.next();
      HDSRangeQuery rangeQuery = e.getValue();
      if (--rangeQuery.windowCountdown < 0) {
        LOG.debug("Removing expired query {}", rangeQuery);
        it.remove(); // query expired
        continue;
      }

      HDSRangeQueryResult res = new HDSRangeQueryResult();
      res.id = rangeQuery.id;
      res.countDown = rangeQuery.windowCountdown;
      res.data = Lists.newArrayListWithExpectedSize(rangeQuery.points.size());
      rangeQuery.prototype.setTimestamp(rangeQuery.startTime);
      for (HDSQuery query : rangeQuery.points) {
        // check in-flight memory store first
        Map<MapAggregate, MapAggregate> buffered = this.cache.get(rangeQuery.prototype.getTimestamp());
        if (buffered != null) {
          MapAggregate ae = buffered.get(rangeQuery.prototype);
          if (ae != null) {
            LOG.debug("Adding from aggregation buffer {}" + ae);
            res.data.add(ae.fields);
            rangeQuery.prototype.setTimestamp(rangeQuery.prototype.getTimestamp() + rangeQuery.intervalTimeUnit.toMillis(1));
            continue;
          }
        }
        // results from persistent store
        if (query.processed && query.result != null) {
          MapAggregate ae = codec.fromKeyValue(query.key.buffer, query.result);
          if (ae.fields != null)
            res.data.add(ae.fields);
        }
        rangeQuery.prototype.setTimestamp(rangeQuery.prototype.getTimestamp() + rangeQuery.intervalTimeUnit.toMillis(1));
      }
      if (!res.data.isEmpty()) {
        LOG.debug("Emitting {} points for {}", res.data.size(), res.id);
        queryResult.emit(res);
      }
    }
  }



  @Override public void setup(Context.OperatorContext arg0)
  {
    super.setup(arg0);
    this.serializer = new GenericEventSerializer(getEventSchema());
    setAggregator(new MapAggregator(getEventSchema()));
  }

  private static final Logger LOG = LoggerFactory.getLogger(HDSQueryOperator.class);
}
