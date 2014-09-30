package com.datatorrent.demos.adsdimension.generic;

import junit.framework.Assert;
import org.junit.Test;

public class GenericDimensionComputationTest
{
  @Test
  public void test()
  {
    GenericDimensionComputation dc = new GenericDimensionComputation();
    dc.setEventSchemaJSON(GenericEventSerializerTest.TEST_SCHEMA_JSON);
    dc.setup(null);

    Assert.assertEquals("Total number of aggregators ", 8, dc.getAggregators().length);
  }
}
