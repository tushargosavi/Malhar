package com.datatorrent.demos.adsdimension.generic;

import com.google.common.collect.Lists;

import java.util.List;

public class DimensionsGenerator
{
  private EventSchema schema;

  public DimensionsGenerator(EventSchema schema)
  {
    this.schema = schema;
  }

  MapAggregator[] generateAggregators()
  {
    if (schema.dimensions == null || schema.dimensions.size() == 0)
    {
      return generateAllAggregators();
    }

    int numDimensions = schema.dimensions.size();
    MapAggregator[] aggregators = new MapAggregator[numDimensions];

    for(int i = 0; i < numDimensions; i++)
    {
      aggregators[i] = new MapAggregator(schema);
      aggregators[i].init(schema.dimensions.get(i));
    }
    return aggregators;
  }

  /**
   * Generate all dimensions from set of keys.
   * @return
   */
  MapAggregator[] generateAllAggregators()
  {

    List<String> keys = Lists.newArrayListWithCapacity(schema.keys.size());
    for(String key : schema.keys)
    {
      if (key.equals(MapAggregate.TIMESTAMP_KEY_STR))
        continue;
      keys.add(key);
    }
    int numKeys = keys.size();
    int numDimensions = 1 << numKeys;
    MapAggregator[] aggregators = new MapAggregator[numDimensions];

    for(int i = 0; i < numDimensions; i++)
    {
      StringBuilder builder = new StringBuilder("time=MINUTES");
      aggregators[i] = new MapAggregator(schema);
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
