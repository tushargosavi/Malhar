package com.datatorrent.demos.waltest;

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
      String key = "key " + i;
      String val = "value " + i;
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
