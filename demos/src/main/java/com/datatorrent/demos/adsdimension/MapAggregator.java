package com.datatorrent.demos.adsdimension;

import com.datatorrent.lib.statistics.DimensionsComputation;
import com.google.common.collect.Maps;

import java.util.Map;

class MapAggregateEvent implements DimensionsComputation.AggregateEvent
{
  Map<String, Object> fields = Maps.newHashMap();
  int aggregatorIndex;

  MapAggregateEvent(int aggregatorIndex)
  {
    this.aggregatorIndex = aggregatorIndex;
  }

  @Override public int getAggregatorIndex()
  {
    return aggregatorIndex;
  }
}


public class MapAggregator implements DimensionsComputation.Aggregator<Map<String, Object>, MapAggregateEvent>
{
  EventDescription eDesc;

  public MapAggregator(EventDescription eDesc)
  {
    this.eDesc = eDesc;
  }

  @Override public MapAggregateEvent getGroup(Map<String, Object> src, int aggregatorIndex)
  {
    MapAggregateEvent aggr = new MapAggregateEvent(aggregatorIndex);
    for(String key : eDesc.keys) {
      aggr.fields.put(key, src.get(key));
    }
    return aggr;
  }

  @Override public void aggregate(MapAggregateEvent dest, Map<String, Object> src)
  {
    for(String metric : eDesc.metrices) {
      dest.fields.put(metric, apply(metric, dest.fields.get(metric), src.get(metric)));
    }
  }

  /* Apply operator between multiple objects */
  private Object apply(String metric, Object o, Object o1)
  {
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
      }
      if (eDesc.dataDesc.get(metric).equals(Double.class))
        return new Double(((Double)o).longValue() + ((Double)o1).longValue());
    }
    return null;
  }

  @Override public void aggregate(MapAggregateEvent dest, MapAggregateEvent src)
  {
    for(String metric : eDesc.metrices) {
      dest.fields.put(metric, apply(metric, dest.fields.get(metric), src.fields.get(metric)));
    }
  }

  // only check keys.
  @Override
  public int computeHashCode(Map<String, Object> stringObjectMap)
  {
    int hashCode = 0;
    for(String key : eDesc.keys)
      if (stringObjectMap.get(key) != null)
        hashCode = 81 * stringObjectMap.get(key).hashCode();

    return hashCode;
  }

  // checks if keys are equal
  @Override
  public boolean equals(Map<String, Object> stringObjectMap, Map<String, Object> stringObjectMap2)
  {
    for(String key : eDesc.keys) {
      Object o1 = stringObjectMap.get(key);
      Object o2 = stringObjectMap2.get(key);
      if (o1 == null && o2 == null)
        continue;
      if (o1 == null || !o1.equals(o2))
        return false;
    }
    return true;
  }
}
