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
  // Set default schema to ADS
  private String eventSchemaJSON = EventSchema.DEFAULT_SCHEMA_ADS;
  private transient EventSchema eventSchema;

  public String getEventSchemaJSON()
  {
    return eventSchemaJSON;
  }

  public void setEventSchemaJSON(String eventSchemaJSON)
  {
    this.eventSchemaJSON = eventSchemaJSON;
    DimensionsGenerator gen = new DimensionsGenerator(getEventSchema());
    MapAggregator[] aggregators = gen.generateAggregators();
    setAggregators(aggregators);
  }

  public EventSchema getEventSchema() {
    if (eventSchema == null ) {
      try {
        eventSchema = EventSchema.createFromJSON(eventSchemaJSON);
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to parse JSON input: " + eventSchemaJSON, e);
      }
    }
    return eventSchema;
  }


  @Override public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    DimensionsGenerator gen = new DimensionsGenerator(getEventSchema());
    MapAggregator[] aggregators = gen.generateAggregators();
    setAggregators(aggregators);
  }
}
