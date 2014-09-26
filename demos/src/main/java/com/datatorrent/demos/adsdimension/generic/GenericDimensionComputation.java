package com.datatorrent.demos.adsdimension.generic;

import com.datatorrent.api.Context;
import com.datatorrent.lib.statistics.DimensionsComputation;

import java.util.Map;

/**
 * GenericDimensionComputation
 *
 * This class takes a schema description and use that to generate aggregators internally
 * during setup.
 *
 * If schema does not specify dimensions, then it generates aggregators for all combinations
 * of keys.
 */
public class GenericDimensionComputation extends DimensionsComputation<Map<String, Object>, MapAggregate>
{
  // TODO , generate schema from json string in setup.
  private EventSchema schema;

  public EventSchema getSchema()
  {
    return schema;
  }

  public void setSchema(EventSchema schema)
  {
    this.schema = schema;
    DimensionsGenerator gen = new DimensionsGenerator(schema);
    MapAggregator[] aggregators = gen.generateAggregators();
    setAggregators(aggregators);
  }

  @Override public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    //EventSchema schema = EventSchema.fromJson(schemaStr);
    DimensionsGenerator gen = new DimensionsGenerator(schema);
    MapAggregator[] aggregators = gen.generateAggregators();
    setAggregators(aggregators);
  }
}
