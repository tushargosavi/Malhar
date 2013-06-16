/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.demos.performance;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class RandomWordInputModule implements InputOperator
{
  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>(this);
  private transient int count;
  private boolean firstTime;

  @Override
  public void emitTuples()
  {
    if (firstTime) {
      for (int i = count--; i-- > 0;) {
        output.emit(new byte[64]);
      }
      firstTime = false;
    }
    else {
      output.emit(new byte[64]);
      count++;
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    firstTime = true;
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

  private static final Logger logger = LoggerFactory.getLogger(RandomWordInputModule.class);
}