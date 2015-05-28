package com.datatorrent.demos.pi;

import com.datatorrent.api.*;
import com.datatorrent.contrib.store.DefaultKeyValStore;
import com.datatorrent.contrib.store.SlicePair;

public class StoreOperator extends BaseOperator implements Operator.CheckpointListener
{
  public transient DefaultKeyValStore store;

  public transient DefaultInputPort<SlicePair> in = new DefaultInputPort<SlicePair>()
  {
    @Override public void process(SlicePair bytePair)
    {
      store.put(bytePair.key, bytePair.value);
    }
  };


  @Override public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    store = new DefaultKeyValStore();
  }

  @Override public void checkpointed(long l)
  {
    store.flush();
  }

  @Override public void committed(long l)
  {

  }
}
