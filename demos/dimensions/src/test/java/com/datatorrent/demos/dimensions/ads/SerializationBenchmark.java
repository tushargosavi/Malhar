package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.lib.statistics.DimensionsComputation;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import org.junit.Test;

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
    gen.setNumAdvertisers(2000);
    gen.setNumPublishers(100);
    gen.setNumAdUnits(100);
    gen.setTimeRange(20);
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

    dimensions.beginWindow(1);
    int count = 0;
    for(AdInfo ai : sink.collectedTuples) {
      dimensions.data.process(ai);
      System.out.println(count++);
    }
    dimensions.endWindow();
    System.out.println("Emmitted tuples " + sink2.collectedTuples.size());

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
