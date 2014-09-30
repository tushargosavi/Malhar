package com.datatorrent.demos.adsdimension.generic;

import com.google.common.collect.Lists;

import java.util.List;

public class DimensionsGenerator
{
  private EventSchema eventSchema;

  public DimensionsGenerator(EventSchema eventSchema)
  {
    this.eventSchema = eventSchema;
  }

  MapAggregator[] generateAggregators()
  {
    if (eventSchema.dimensions == null || eventSchema.dimensions.size() == 0)
    {
      return generateAllAggregators();
    }

    int numDimensions = eventSchema.dimensions.size();
    MapAggregator[] aggregators = new MapAggregator[numDimensions];

    for(int i = 0; i < numDimensions; i++)
    {
      aggregators[i] = new MapAggregator(eventSchema);
      aggregators[i].init(eventSchema.dimensions.get(i));
    }
    return aggregators;
  }

  /**
   * Generate all dimensions from set of keys.
   * @return
   */
  MapAggregator[] generateAllAggregators()
  {
    if (eventSchema.keys.size() <= 0 ) return null;

    List<String> keys = eventSchema.keys;

    for(String key : keys)
    {
      if (key.equals(eventSchema.getTimestamp()))
        continue;
      keys.add(key);
    }
    int numKeys = keys.size();
    int numDimensions = 1 << numKeys;
    MapAggregator[] aggregators = new MapAggregator[numDimensions];

    for(int i = 0; i < numDimensions; i++)
    {
      StringBuilder builder = new StringBuilder("time=MINUTES");
      aggregators[i] = new MapAggregator(eventSchema);
      for(int k = 0; k < numKeys; k++)
      {
        if ((i & (1 << k)) != 0) {
          builder.append(':');
          builder.append(keys.get(k));
        }
      }
      aggregators[i].init(builder.toString());
    }

    return aggregators;
  }
}
