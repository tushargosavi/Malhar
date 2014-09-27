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

import com.datatorrent.lib.statistics.DimensionsComputation;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class MapAggregate implements DimensionsComputation.AggregateEvent
{

  MapAggregate() {
  }

  public Map<String, Object> fields = Maps.newLinkedHashMap();
  private int aggregatorIndex = 0;
  private EventSchema eventSchema;

  public MapAggregate(EventSchema eventSchema) {
    this.eventSchema = eventSchema;
  }

  public MapAggregate(EventSchema eventSchema, int aggregatorIndex) {
    this.eventSchema = eventSchema;
    this.aggregatorIndex = aggregatorIndex;
  }

  public MapAggregate(EventSchema eventSchema, Map<String, String> event) {
    this.eventSchema = eventSchema;
    for(String keyStr : eventSchema.keys)
    {
      if (event.containsKey(keyStr))
      {
        fields.put(keyStr, eventSchema.typeCast(event.get(keyStr), keyStr));
      }
    }
  }

  public EventSchema getEventSchema() {
    return eventSchema;
  }


  @Override
  public int getAggregatorIndex()
  {
    return aggregatorIndex;
  }

  public long getTimestamp()
  {
    Object o = fields.get(eventSchema.getTimeKey());
    if (o == null) return 0L;
    return ((Number)o).longValue();
  }

  public void setTimestamp(long timestamp)
  {
    fields.put(eventSchema.getTimeKey(), timestamp);
  }

  public Object get(String field)
  {
    return fields.get(field);
  }

  @Override
  public boolean equals(Object o)
  {

    if (this == o) {
      return true;
    }
    if (!(o instanceof MapAggregate)) {
      return false;
    }

    MapAggregate that = (MapAggregate) o;

    for(String key: eventSchema.keys) {
      Object thisValue = fields.get(key);
      Object thatValue = that.fields.get(key);
      if (thisValue != null ? ! thisValue.equals(thatValue) : thatValue != null) {
        return false;
      }
    }

    return true;
  }

  /**
   * Get a subset of fields which represent keys based on the schema
   * @return
   */
  public Map<String, Object> getKeys(){
    return Maps.filterKeys(fields, Predicates.in(eventSchema.keys));
  }

  @Override
  public int hashCode()
  {
    return getKeys().hashCode();
  }

  @Override
  public String toString()
  {
    return "MapAggregate{" +
        "keys=" + eventSchema.keys.toString() +
        ", fields=" + fields +
        ", aggregatorIndex=" + aggregatorIndex +
        '}';
  }
}


public class MapAggregator implements DimensionsComputation.Aggregator<Map<String, Object>, MapAggregate>
{
  private static final long serialVersionUID = 7636266873750826291L;
  private EventSchema eventSchema;
  private String dimension;
  private TimeUnit time;
  private final List<String> keys = Lists.newArrayList();

  public MapAggregator() {}

  public MapAggregator(EventSchema eventSchema)
  {
    this.eventSchema = eventSchema;
  }

  public void init(String dimension)
  {
    String[] attributes = dimension.split(":");
    for (String attribute : attributes) {
      String[] keyval = attribute.split("=", 2);
      String key = keyval[0];
      if (key.equals("time")) {
        time = TimeUnit.valueOf(keyval[1]);
        continue;
      }
      keys.add(key);
    }
    this.dimension = dimension;
  }

  /**
   * Return an MapAggregateEvent with only dimension keys and converted timestamp.
   * @param src
   * @param aggregatorIndex
   * @return
   */
  @Override
  public MapAggregate getGroup(Map<String, Object> src, int aggregatorIndex)
  {
    MapAggregate aggr = new MapAggregate(eventSchema, aggregatorIndex);
    for (String key: keys) {
      aggr.fields.put(key, src.get(key));
    }
    /* Add converted timestamp */
    if (time != null) {
      long timestamp = src.get(eventSchema.getTimeKey()) != null? ((Number)src.get(eventSchema.getTimeKey())).longValue() : 0;
      timestamp = TimeUnit.MILLISECONDS.convert(time.convert(timestamp, TimeUnit.MILLISECONDS), time);
      aggr.fields.put("timestamp", new Long(timestamp));
    }
    return aggr;
  }

  @Override
  public void aggregate(MapAggregate dest, Map<String, Object> src)
  {
    for(String metric : eventSchema.getAggregateKeys()) {
      dest.fields.put(metric, apply(metric, dest.fields.get(metric), src.get(metric)));
    }
  }

  /* Apply operator between multiple objects */
  private Object apply(String metric, Object o, Object o1)
  {
    //TODO define a class for each type of aggregation and
    // avoid if/else.
    if (eventSchema.aggregates.get(metric).equals("sum"))
    {
      if (eventSchema.fieldTypes.get(metric).equals(Integer.class)) {
        int val1 = (o != null) ? ((Number)o).intValue() : 0;
        int val2 = (o1 != null) ? ((Number)o1).intValue() : 0;
        return new Integer(val1 + val2);
      } else if (eventSchema.fieldTypes.get(metric).equals(Long.class)) {
        long val1 = (o != null) ? ((Number)o).longValue() : 0;
        long val2 = (o1 != null) ? ((Number)o1).longValue() : 0;
        return new Long(val1 + val2);
      } else if (eventSchema.fieldTypes.get(metric).equals(Double.class)) {
        double val1 = (o != null) ? ((Number)o).doubleValue() : 0;
        double val2 = (o1 != null) ? ((Number)o1).doubleValue() : 0;
        return new Double(val1 + val2);
      }
    }
    return null;
  }

  @Override
  public void aggregate(MapAggregate dest, MapAggregate src) {
    for (String metric : eventSchema.getAggregateKeys()) {
      dest.fields.put(metric, apply(metric, dest.fields.get(metric), src.fields.get(metric)));
    }
  }

  // only check keys.
  @Override
  public int computeHashCode(Map<String, Object> tuple)
  {
    int hash = 0;
    for(String key : keys)
      if (tuple.get(key) != null)
        hash = 81 * tuple.get(key).hashCode();

    /* TODO: special handling for timestamp */
    if (time != null) {
        long timestamp = tuple.get(getEventSchema().getTimeKey()) != null? ((Number)tuple.get(getEventSchema().getTimeKey())).longValue() : 0;
        long ltime = time.convert(timestamp, TimeUnit.MILLISECONDS);
        hash = 71 * hash + (int) (ltime ^ (ltime >>> 32));
    }
    return hash;
  }

  // checks if keys are equal
  @Override
  public boolean equals(Map<String, Object> event1, Map<String, Object> event2)
  {
    for(String key : keys) {
      Object o1 = event1.get(key);
      Object o2 = event2.get(key);
      if (o1 == null && o2 == null)
        continue;
      if (o1 == null || !o1.equals(o2))
        return false;
    }

    // Special handling for timestamp
    if (time != null)
    {
      long t1 = event1.get(getEventSchema().getTimeKey()) != null? ((Number)event1.get(getEventSchema().getTimeKey())).longValue() : 0;
      long t2 = event2.get(getEventSchema().getTimeKey()) != null? ((Number)event2.get(getEventSchema().getTimeKey())).longValue() : 0;

      if (time.convert(t1, TimeUnit.MILLISECONDS) != time.convert(t2, TimeUnit.MILLISECONDS))
        return false;
    }
    return true;
  }

  public EventSchema getEventSchema()
  {
    return eventSchema;
  }

  public String getDimension()
  {
    return dimension;
  }

  public TimeUnit getTime()
  {
    return time;
  }

  public List<String> getKeys()
  {
    return keys;
  }
}
