package com.datatorrent.demos.adsdimension;

import com.datatorrent.lib.statistics.DimensionsComputation;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.velocity.runtime.parser.node.MapGetExecutor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class MapAggregateEvent implements DimensionsComputation.AggregateEvent
{
  protected static final String TIMESTAMP_KEY_STR = "timestamp";

  Map<String, Object> keys = Maps.newHashMap();
  Map<String, Object> fields = Maps.newHashMap();
  int aggregatorIndex;

  protected MapAggregateEvent() {}
  public MapAggregateEvent(int aggregatorIndex)
  {
    this.aggregatorIndex = aggregatorIndex;
  }

  @Override public int getAggregatorIndex()
  {
    return aggregatorIndex;
  }

  public Long getTimestamp()
  {
    Object o = keys.get(TIMESTAMP_KEY_STR);
    if (o == null)
      return 0L;

    return ((Long)o).longValue();
  }

  public void setTimestamp(long timestamp)
  {
    keys.put(TIMESTAMP_KEY_STR, timestamp);
  }

  public Object get(String field)
  {
    if (keys.containsKey(field))
      return keys.get(field);
    if (fields.containsKey(field))
      return fields.get(field);
    return null;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MapAggregateEvent)) {
      return false;
    }

    MapAggregateEvent that = (MapAggregateEvent) o;

    if (keys == null && that.keys == null)
      return true;

    if (keys == null)
      return false;

    return keys.equals(that.keys);
  }

  @Override
  public int hashCode()
  {
    int result = keys != null ? keys.hashCode() : 0;
    return result;
  }
}


public class MapAggregator implements DimensionsComputation.Aggregator<Map<String, Object>, MapAggregateEvent>
{
  transient protected EventDescription eDesc;
  protected String dimension;
  protected TimeUnit time;
  protected List<String> keys = Lists.newArrayList();

  public MapAggregator() {}

  public MapAggregator(EventDescription eDesc)
  {
    this.eDesc = eDesc;
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

  @Override public MapAggregateEvent getGroup(Map<String, Object> src, int aggregatorIndex)
  {
    if (eDesc == null)
      eDesc = EventDescription.getDefault();
    MapAggregateEvent aggr = new MapAggregateEvent(aggregatorIndex);
    for(String key : keys) {
      aggr.keys.put(key, src.get(key));
    }
    /* Add converted timestamp */
    if (time != null) {
      long timestamp = src.get(MapAggregateEvent.TIMESTAMP_KEY_STR) != null? ((Long)src.get(MapAggregateEvent.TIMESTAMP_KEY_STR)).longValue() : 0;
      timestamp = TimeUnit.MILLISECONDS.convert(time.convert(timestamp, TimeUnit.MILLISECONDS), time);
      aggr.keys.put("timestamp", new Long(timestamp));
    }
    return aggr;
  }

  @Override public void aggregate(MapAggregateEvent dest, Map<String, Object> src)
  {
    if (eDesc == null)
      eDesc = EventDescription.getDefault();
    for(String metric : eDesc.metrices) {
      dest.fields.put(metric, apply(metric, dest.fields.get(metric), src.get(metric)));
    }
  }

  /* Apply operator between multiple objects */
  private Object apply(String metric, Object o, Object o1)
  {
    if (eDesc == null)
      eDesc = EventDescription.getDefault();
    if (eDesc.aggrDesc.get(metric).equals("sum"))
    {
      if (eDesc.dataDesc.get(metric).equals(Integer.class)) {
        int val1 = (o != null) ? ((Integer)o).intValue() : 0;
        int val2 = (o1 != null) ? ((Integer)o1).intValue() : 0;
        return new Integer(val1 + val2);
      } else if (eDesc.dataDesc.get(metric).equals(Long.class)) {
        long val1 = (o != null) ? ((Long)o).longValue() : 0;
        long val2 = (o1 != null) ? ((Long)o1).longValue() : 0;
        return new Long(val1 + val2);
      } else if (eDesc.dataDesc.get(metric).equals(Double.class)) {
        double val1 = (o != null) ? ((Double)o).doubleValue() : 0;
        double val2 = (o1 != null) ? ((Double)o1).doubleValue() : 0;
        return new Double(val1 + val2);
      }
    }
    return null;
  }

  @Override public void aggregate(MapAggregateEvent dest, MapAggregateEvent src)
  {
    if (eDesc == null)
      eDesc = EventDescription.getDefault();
    for(String metric : eDesc.metrices) {
      dest.fields.put(metric, apply(metric, dest.fields.get(metric), src.fields.get(metric)));
    }
  }

  // only check keys.
  @Override
  public int computeHashCode(Map<String, Object> tuple)
  {
    if (eDesc == null)
      eDesc = EventDescription.getDefault();
    int hash = 0;
    for(String key : keys)
      if (tuple.get(key) != null)
        hash = 81 * tuple.get(key).hashCode();

    /* TODO: special handling for timestamp */
    if (time != null) {
        long timestamp = tuple.get(MapAggregateEvent.TIMESTAMP_KEY_STR) != null? ((Long)tuple.get(MapAggregateEvent.TIMESTAMP_KEY_STR)).longValue() : 0;
        long ltime = time.convert(timestamp, TimeUnit.MILLISECONDS);
        hash = 71 * hash + (int) (ltime ^ (ltime >>> 32));
    }
    return hash;
  }

  // checks if keys are equal
  @Override
  public boolean equals(Map<String, Object> event1, Map<String, Object> event2)
  {
    if (eDesc == null)
      eDesc = EventDescription.getDefault();
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
      long t1 = event1.get(MapAggregateEvent.TIMESTAMP_KEY_STR) != null? ((Long)event1.get(MapAggregateEvent.TIMESTAMP_KEY_STR)).longValue() : 0;
      long t2 = event2.get(MapAggregateEvent.TIMESTAMP_KEY_STR) != null? ((Long)event2.get(MapAggregateEvent.TIMESTAMP_KEY_STR)).longValue() : 0;

      if (time.convert(t1, TimeUnit.MILLISECONDS) != time.convert(t2, TimeUnit.MILLISECONDS))
        return false;
    }
    return true;
  }

}
