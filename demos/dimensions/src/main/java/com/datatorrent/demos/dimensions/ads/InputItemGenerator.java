/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.dimensions.ads;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.Min;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;


/**
 * <p>InputItemGenerator class.</p>
 *
 * @displayName Input Item Generator
 * @category Input
 * @tags generator, input operator
 * @since 0.3.2
 */
public class InputItemGenerator implements InputOperator
{
  @Min(1)
  private int numPublishers = 50;
  @Min(1)
  private int numAdvertisers = 100;
  @Min(1)
  private int numAdUnits = 5;
  private double expectedClickThruRate = 0.005;
  @Min(1)
  private int blastCount = 10000;
  @Min(1)
  private int rate = 1;
  transient private int count;

  /* Generate data for older timestamp */
  private transient int timeRange = 0;

  private final Random random = new Random();
  public final transient DefaultOutputPort<AdInfo> outputPort = new DefaultOutputPort<AdInfo>();

  public double getExpectedClickThruRate()
  {
    return expectedClickThruRate;
  }

  public void setExpectedClickThruRate(double expectedClickThruRate)
  {
    this.expectedClickThruRate = expectedClickThruRate;
  }

  public int getBlastCount()
  {
    return blastCount;
  }

  public void setBlastCount(int blastCount)
  {
    this.blastCount = blastCount;
  }

  public int getNumPublishers()
  {
    return numPublishers;
  }

  public void setNumPublishers(int numPublishers)
  {
    this.numPublishers = numPublishers;
  }

  public int getNumAdvertisers()
  {
    return numAdvertisers;
  }

  public void setNumAdvertisers(int numAdvertisers)
  {
    this.numAdvertisers = numAdvertisers;
  }

  public int getNumAdUnits()
  {
    return numAdUnits;
  }

  public void setNumAdUnits(int numAdUnits)
  {
    this.numAdUnits = numAdUnits;
  }

  public int getRate()
  {
    return rate;
  }

  public void setRate(int rate)
  {
    this.rate = rate;
  }

  public int getTimeRange()
  {
    return timeRange;
  }

  public void setTimeRange(int timeRange)
  {
    this.timeRange = timeRange;
  }

  @Override
  public void beginWindow(long windowId)
  {
    count = rate;
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  private int nextRandomId(int max)
  {
    int id;
    do {
      id = (int)Math.abs(Math.round(random.nextGaussian() * max / 2));
    }
    while (id >= max);
    return id;
  }

  private AdInfo generateAdInfo()
  {
    AdInfo adInfo = new AdInfo();
    adInfo.setPublisherId(nextRandomId(numPublishers) + 1);
    adInfo.setAdvertiserId(nextRandomId(numAdvertisers) + 1);
    adInfo.setAdUnit(random.nextInt(numAdUnits) + 1);
    adInfo.cost = 0.5 + 0.25 * random.nextDouble();
    adInfo.impressions = 1;

    long timestamp = System.currentTimeMillis();
    /* Generate timestamp with normal distribution */
    long minDiff = (long)(Math.abs(random.nextGaussian()) * (double)timeRange/2.0);
    long delta = TimeUnit.MILLISECONDS.convert(minDiff, TimeUnit.MINUTES);
    timestamp -= delta;
    adInfo.setTimestamp(timestamp);

    /* 0 (zero) is used as the invalid value */
    return adInfo;
  }

  private AdInfo generateClickEvent(AdInfo ai)
  {
    AdInfo adInfo = new AdInfo(ai);
    adInfo.revenue = 0.5 + 0.5 * random.nextDouble();
    adInfo.clicks = 1;
    adInfo.cost = 0;
    adInfo.impressions = 0;
    return adInfo;
  }

  @Override
  public void emitTuples()
  {
    try {
      int numTuples = Math.min(blastCount, count);
      count -= numTuples;
      for (int i = 0; i < numTuples; ++i) {
        AdInfo ai = generateAdInfo();
        emitTuple(ai);
        if (random.nextDouble() < expectedClickThruRate) {
          ai = generateClickEvent(ai);
          emitTuple(ai);
          i++;
        }
      }
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }


  public void emitTuple(AdInfo adInfo) {
    this.outputPort.emit(adInfo);
  }

}
