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
import java.util.concurrent.TimeUnit;

public class Generator extends BaseOperator implements InputOperator
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
  private int keyGenType;

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

  public int getKeyGenType()
  {
    return keyGenType;
  }

  public void setKeyGenType(int keyGenType)
  {
    this.keyGenType = keyGenType;
  }

  private static final Random random = new Random();

  private transient KeyGenerator keyGen = null;

  @Override public void emitTuples()
  {
    long timestamp = System.currentTimeMillis();

    for(int i = 0; i < tupleBlast; i++)
    {

      byte[] key = keyGen.generateKey(timestamp, i);
      ByteBuffer.wrap(val).putLong(random.nextLong());
      MutableKeyValue pair = new MutableKeyValue(key, val);
      out.emit(pair);
    }
    try {
      if (sleepms != 0)
        Thread.sleep(sleepms);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  @Override public void setup(Context.OperatorContext operatorContext)
  {
    val = ByteBuffer.allocate(valLen).putLong(1234).array();
    keyGen = createGenerator(keyGenType);
  }

  private KeyGenerator createGenerator(int genType)
  {
    switch (genType) {
    case 0 :
    {
      PureRandomGen gen = new PureRandomGen();
      gen.setRange(cardinality);
      return gen;
    }
    case 1 :
    {
      HistoricalOneMinuteGen gen = new HistoricalOneMinuteGen();
      gen.setRange(cardinality);
      return gen;
    }
    case 2 :
    {
      HotKeyGenerator gen = new HotKeyGenerator();
      gen.setRange(cardinality);
      return gen;
    }
    case 3 :
    {
      return new SequenceKeyGenerator();
    }
    default:
      throw new RuntimeException("Not supported Generator" + genType);
    }
  }
  
  static interface KeyGenerator {
    byte[] generateKey(long timestamp, int i);
  }

  static abstract class AbstractKeyGenerator implements KeyGenerator {
    long range;

    public long getRange()
    {
      return range;
    }

    public void setRange(long range)
    {
      this.range = range;
    }
  }

  static class PureRandomGen extends AbstractKeyGenerator {

    @Override public byte[] generateKey(long timestamp, int i)
    {
      int val = 0;
      if (range != 0) val = random.nextInt((int)range);
      else val = random.nextInt();
      return ByteBuffer.allocate(8).putLong(val).array();
    }
  }

  static class HistoricalOneMinuteGen extends AbstractKeyGenerator {
    @Override public byte[] generateKey(long timestamp, int i)
    {
      long minute = TimeUnit.MINUTES.convert(timestamp, TimeUnit.MILLISECONDS);
      long longKey = Math.abs(random.nextLong());
      longKey = range == 0? longKey : longKey % range;
      byte[] key = ByteBuffer.allocate(16).putLong(minute).putLong(longKey).array();
      return key;
    }
  }


  static class HotKeyGenerator extends AbstractKeyGenerator {
    @Override public byte[] generateKey(long timestamp, int i)
    {
      byte[] key = ByteBuffer.allocate(16).putLong((timestamp - timestamp % range) + random.nextInt((int)range)).putLong(i).array();
      return key;
    }
  }

  static class SequenceKeyGenerator extends AbstractKeyGenerator {
    @Override public byte[] generateKey(long timestamp, int i)
    {
      return ByteBuffer.allocate(16).putLong(timestamp).putInt(i).array();
    }
  }

}
