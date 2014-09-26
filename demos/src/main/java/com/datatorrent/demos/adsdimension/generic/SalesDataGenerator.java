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
package com.datatorrent.demos.adsdimension.generic;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import javax.validation.constraints.Min;
import java.util.Random;

/**
 * <p>InputItemGenerator class.</p>
 *
 * @since 0.3.2
 */
public class SalesDataGenerator implements InputOperator
{

  public static class SaleInfo
  {
    public int productId;
    public int customerId;
    public int channelId;

    public long amount;
    public long timestamp;
  }

  @Min(1)
  private int numProducts = 50;
  @Min(1)
  private int numCustomers = 100;
  @Min(1)
  private int numChannels = 5;

  private int blastCount = 10000;
  private final Random random = new Random();
  @OutputPortFieldAnnotation(name = "outputPort")
  public final transient DefaultOutputPort<SaleInfo> outputPort = new DefaultOutputPort<SaleInfo>();

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
    return numProducts;
  }

  public void setNumPublishers(int numPublishers)
  {
    this.numProducts = numPublishers;
  }

  public int getNumCustomers()
  {
    return numCustomers;
  }

  public void setNumCustomers(int numCustomers)
  {
    this.numCustomers = numCustomers;
  }

  public int getNumChannels()
  {
    return numChannels;
  }

  public void setNumChannels(int numChannels)
  {
    this.numChannels = numChannels;
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(Context.OperatorContext context)
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

  @Override
  public void emitTuples()
  {
    try {
      long timestamp;
      for (int i = 0; i < blastCount; ++i) {
        int customerId = nextRandomId(numCustomers);
        int productId = nextRandomId(numProducts);
        int channelId = random.nextInt(numChannels);
        timestamp = System.currentTimeMillis();

        long cost = random.nextInt(100);

        /* 0 (zero) is used as the invalid value */
        buildAndSend(false, productId + 1, customerId + 1, channelId + 1, cost, timestamp);

      }
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }


  public void emitTuple(SaleInfo adInfo) {
    this.outputPort.emit(adInfo);
  }

  private void buildAndSend(boolean click, int productId, int customerId, int channelId, long amount, long timestamp)
  {
    SaleInfo saleInfo = new SaleInfo();
    saleInfo.productId = productId;
    saleInfo.customerId = customerId;
    saleInfo.channelId = channelId;
    saleInfo.amount = amount;
    saleInfo.timestamp = timestamp;

    emitTuple(saleInfo);
  }

}
