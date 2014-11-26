package com.datatorrent.demos.dimensions.ads;

import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.Map;

public class InputGenTest
{
  @Test
  public void test()
  {
    Random r = new Random();
    long ts = System.currentTimeMillis();
    long minute = TimeUnit.MINUTES.convert(ts, TimeUnit.MILLISECONDS);
    long timeRange = 3;
    Map<Long, Integer> counts = Maps.newTreeMap();

    for(int i = 0; i < 1000; i++) {
      //
      //long minDiff = (long)((r.nextGaussian() * ((double)timeRange / 2.0)) + ((double)timeRange / 2.0));
      long minDiff = (long)(Math.abs(r.nextGaussian()) * (double)timeRange/2.0);
      long delta = TimeUnit.MILLISECONDS.convert(minDiff, TimeUnit.MINUTES);
      long nts = ts - delta;
      long m = TimeUnit.MINUTES.convert(nts, TimeUnit.MILLISECONDS);
      if (counts.containsKey(m)) {
        counts.put(m, counts.get(m) + 1);
      } else
        counts.put(m, 1);
    }
    System.out.println("current " + minute);
    System.out.println(counts);
  }
}
