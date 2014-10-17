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
package com.datatorrent.benchmark.waltest;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.contrib.hds.MutableKeyValue;

public class Generator implements InputOperator
{
  public int tupleBlast = 100;
  public int sleepms = 100;

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

  @Override public void emitTuples()
  {
    for(int i = 0; i < tupleBlast; i++)
    {
      String key = "key   " + String.format("%05d", i);
      String val = "value " + String.format("%05d", i);
      MutableKeyValue pair = new MutableKeyValue(key.getBytes(), val.getBytes());
      out.emit(pair);
    }
    try {
      Thread.sleep(sleepms);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  @Override public void beginWindow(long l)
  {

  }

  @Override public void endWindow()
  {

  }

  @Override public void setup(Context.OperatorContext operatorContext)
  {

  }

  @Override public void teardown()
  {

  }
}
