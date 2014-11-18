/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.dimensions.benchmark;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.contrib.hds.MutableKeyValue;

import javax.validation.constraints.Min;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.*;

public class FixedRateGenerator extends BaseOperator implements InputOperator
{
  /* Following parameters controls rate of tuples generation */
  private int tupleBlast = 1000;
  private int sleepms = 0;

  /* Length of value */
  @Min(8)
  private int valLen = 1000;

  /* Cardinality of keys, without timestamp filed */
  private long cardinality = 10000;


  private transient byte[] val;


  public int getTupleBlast()
  {
    return tupleBlast;
  }

  public void setTupleBlast(int tupleBlast)
  {
    this.tupleBlast = tupleBlast;
  }

  public int getSleepms()
  {
    return sleepms;
  }

  public void setSleepms(int sleepms)
  {
    this.sleepms = sleepms;
  }

  public transient DefaultOutputPort<MutableKeyValue> out = new DefaultOutputPort<MutableKeyValue>();

  public int getValLen()
  {
    return valLen;
  }

  public void setValLen(int valLen)
  {
    this.valLen = valLen;
  }

  public long getCardinality()
  {
    return cardinality;
  }

  public void setCardinality(long cardinality)
  {
    this.cardinality = cardinality;
  }

  private static final Random random = new Random();

  @Override public void emitTuples()
  {
    for (int i = 0; i < 100; i++) {
      if (blockingQueue != null)
        try {
          MutableKeyValue val = blockingQueue.poll(sleepms, TimeUnit.MILLISECONDS);
          if (val != null)
            out.emit(val);
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException("Exception for me");
        }
    }
  }

  private MutableKeyValue generateMutableKeyValue()
  {
    long timestamp = TimeUnit.MINUTES.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    long longKey = Math.abs(random.nextLong());
    longKey = cardinality == 0? longKey : longKey % cardinality;
    byte[] key = ByteBuffer.allocate(16).putLong(timestamp).putLong(longKey).array();
    ByteBuffer.wrap(val).putLong(random.nextLong());
    return new MutableKeyValue(key, val);
  }

  private transient ScheduledExecutorService ses = Executors.newScheduledThreadPool(5);
  @Override public void setup(Context.OperatorContext operatorContext)
  {
    val = ByteBuffer.allocate(valLen).putLong(1234).array();
    blockingQueue = new LinkedBlockingDeque<MutableKeyValue>(genRate * 2);
    ses.scheduleAtFixedRate(genratorThread, 10, 1, TimeUnit.SECONDS);
  }

  private int genRate = 30000;
  BlockingQueue<MutableKeyValue> blockingQueue = null;

  private transient Runnable genratorThread = new Runnable()
  {
    @Override public void run()
    {
      int k = 0;
      while(k < genRate) {
        k++;
        MutableKeyValue val = generateMutableKeyValue();
        try {
          blockingQueue.put(val);
        } catch (InterruptedException e) {
          throw new RuntimeException("Exception while adding item");
        }
      }
    }
  };

}
