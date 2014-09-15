package com.datatorrent.demos.adsdimension;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class GenericAggregatorTest
{
  @Test
  public void test() {
    MapAggregator aggregator = new MapAggregator(GenericEventSerializerTest.getDataDesc());

    /* prepare a object */
    Map<String, Object> event = Maps.newHashMap();
    event.put("timestamp", System.currentTimeMillis());
    event.put("pubId", 1);
    event.put("adUnit", 2);
    event.put("adId", 3);
    event.put("clicks", new Long(10));

    MapAggregateEvent aggr = aggregator.getGroup(event, 0);
    aggregator.aggregate(aggr, event);

    /* prepare a object */
    Map<String, Object> event2 = Maps.newHashMap();
    event2.put("timestamp", System.currentTimeMillis());
    event2.put("pubId", 1);
    event2.put("adUnit", 2);
    event2.put("adId", 3);
    event2.put("clicks", new Long(20));

    aggregator.aggregate(aggr, event2);

    Assert.assertEquals("sum is 30", aggr.fields.get("clicks"), 30L);
  }
}
