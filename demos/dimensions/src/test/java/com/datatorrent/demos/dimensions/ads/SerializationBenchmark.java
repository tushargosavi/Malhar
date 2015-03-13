package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.lib.statistics.DimensionsComputation;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SerializationBenchmark
{
  @Test
  public void testSerialization()
  {
    InputItemGenerator gen = new InputItemGenerator();
    CollectorTestSink<AdInfo> sink = new CollectorTestSink<AdInfo>();
    TestUtils.setSink(gen.outputPort, sink);
    gen.setBlastCount(1000);
    gen.setRate(1000);
    gen.setNumAdvertisers(100);
    gen.setNumPublishers(50);
    gen.setNumAdUnits(5);
    gen.setTimeRange(0);
    gen.beginWindow(1);
    gen.emitTuples();
    gen.endWindow();

    System.out.println(sink.collectedTuples.size());

    AdInfo.AdInfoAggregateEvent ae = new AdInfo.AdInfoAggregateEvent();
    ae.publisherId = 1;
    ae.aggregatorIndex = 1;
    ae.setAdUnit(2);
    ae.setAdvertiserId(4);
    ae.setClicks(1);

    String[] dimensionSpecs = new String[] {
      "time=" + TimeUnit.MINUTES,
      "time=" + TimeUnit.MINUTES + ":adUnit",
      "time=" + TimeUnit.MINUTES + ":advertiserId",
      "time=" + TimeUnit.MINUTES + ":publisherId",
      "time=" + TimeUnit.MINUTES + ":advertiserId:adUnit",
      "time=" + TimeUnit.MINUTES + ":publisherId:adUnit",
      "time=" + TimeUnit.MINUTES + ":publisherId:advertiserId",
      "time=" + TimeUnit.MINUTES + ":publisherId:advertiserId:adUnit"
    };

    AdInfo.AdInfoAggregator[] aggregators = new AdInfo.AdInfoAggregator[dimensionSpecs.length];
    for (int i = dimensionSpecs.length; i-- > 0;) {
      AdInfo.AdInfoAggregator aggregator = new AdInfo.AdInfoAggregator();
      aggregator.init(dimensionSpecs[i]);
      aggregators[i] = aggregator;
    }

    DimensionsComputation<AdInfo, AdInfo.AdInfoAggregateEvent> dimensions = new DimensionsComputation<AdInfo, AdInfo.AdInfoAggregateEvent>();
    dimensions.setAggregators(aggregators);

    CollectorTestSink<AdInfo.AdInfoAggregateEvent> sink2 = new CollectorTestSink<AdInfo.AdInfoAggregateEvent>();
    TestUtils.setSink(dimensions.output, sink2);

    Set<Integer> pubSet = Sets.newHashSet();
    Set<Integer> adSet = Sets.newHashSet();
    Set<Integer> unitSet = Sets.newHashSet();
    Set<Long> minuteSet = Sets.newHashSet();

    for(AdInfo ai : sink.collectedTuples) {
      pubSet.add(ai.getPublisherId());
      adSet.add(ai.getAdvertiserId());
      unitSet.add(ai.getAdUnit());
      TimeUnit time = TimeUnit.MINUTES;
      long minutes = TimeUnit.MILLISECONDS.convert(time.convert(ai.timestamp, TimeUnit.MILLISECONDS), time);
      minuteSet.add(minutes);
    }

    System.out.println("cardinality");
    System.out.println("publishers " + pubSet.size());
    System.out.println("adSet " + adSet.size());
    System.out.println("unitSize " + unitSet.size());
    System.out.println("Minutes " + minuteSet.size());
    System.out.println("total " + pubSet.size() * adSet.size() * unitSet.size() + minuteSet.size());


    dimensions.beginWindow(1);
    for(AdInfo ai : sink.collectedTuples) {
      dimensions.data.process(ai);
    }
    dimensions.endWindow();
    System.out.println("Emitted tuples " + sink2.collectedTuples.size());

    /*
    AdsDimensionStoreOperator.AdInfoAggregateCodec codec = new AdsDimensionStoreOperator.AdInfoAggregateCodec();
    codec.operator = new AdsDimensionStoreOperator();

    long start = System.currentTimeMillis();
    for(int i = 0; i < 30000; i++) {
      byte[] key = codec.getKeyBytes(ae);
      byte[] value = codec.getValueBytes(ae);
      AdInfo.AdInfoAggregateEvent ae1 = codec.fromKeyValue(new Slice(key), value);
    }
    System.out.println("time duration " + (System.currentTimeMillis() - start));
    */

  }


}
