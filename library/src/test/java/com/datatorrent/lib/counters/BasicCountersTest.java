package com.datatorrent.lib.counters;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableDouble;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.datatorrent.lib.util.NumberAggregate;

/**
 * Tests for {@link BasicCounters}
 */
public class BasicCountersTest
{
  private static enum CounterKeys
  {
    A
  }

  @Test
  public void testBasicCounters() throws InstantiationException, IllegalAccessException
  {
    BasicCounters<MutableDouble> doubleBasicCounters = new BasicCounters<MutableDouble>(MutableDouble.class);
    MutableDouble counterA = doubleBasicCounters.findCounter(CounterKeys.A);

    counterA.increment();

    MutableDouble counterAInCounters = doubleBasicCounters.getCounter(CounterKeys.A);
    Assert.assertNotNull("null", doubleBasicCounters.getCounter(CounterKeys.A));
    Assert.assertTrue("equality", counterAInCounters == counterA);
    Assert.assertEquals(counterA.doubleValue(), 1.0, 0);
  }

  @Test
  public void testBasicCountersAggregator() throws InstantiationException, IllegalAccessException
  {
    List<Object> physicalCounters = Lists.newArrayList();

    for (int i = 0; i < 5; i++) {
      BasicCounters<MutableDouble> doubleBasicCounters = new BasicCounters<MutableDouble>(MutableDouble.class);
      MutableDouble counterA = doubleBasicCounters.findCounter(CounterKeys.A);
      counterA.increment();

      physicalCounters.add(doubleBasicCounters);
    }

    BasicCounters.DoubleAggregator<MutableDouble> aggregator = new BasicCounters.DoubleAggregator<MutableDouble>();
    @SuppressWarnings("unchecked")
    Map<String, NumberAggregate.DoubleAggregate> aggregateMap = (Map<String, NumberAggregate.DoubleAggregate>) aggregator.aggregate(physicalCounters);

    Assert.assertNotNull("null", aggregateMap.get(CounterKeys.A.name()));
    NumberAggregate.DoubleAggregate aggregate = aggregateMap.get(CounterKeys.A.name());

    Assert.assertEquals(aggregate.getSum().doubleValue(), 5.0, 0);
    Assert.assertEquals(aggregate.getMin().doubleValue(), 1.0, 0);
    Assert.assertEquals(aggregate.getMax().doubleValue(), 1.0, 0);
    Assert.assertEquals(aggregate.getAvg().doubleValue(), 1.0, 0);
  }
}
