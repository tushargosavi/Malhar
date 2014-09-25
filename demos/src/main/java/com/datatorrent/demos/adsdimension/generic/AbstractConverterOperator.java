package com.datatorrent.demos.adsdimension.generic;

import com.datatorrent.api.*;
import com.datatorrent.lib.datamodel.converter.Converter;

public abstract class AbstractConverterOperator<INPUT, OUTPUT> extends BaseOperator implements Converter<INPUT, OUTPUT>
{
  public transient DefaultOutputPort<OUTPUT> outputPort = new DefaultOutputPort<OUTPUT>();

  public transient DefaultInputPort<INPUT> inputPort = new DefaultInputPort<INPUT>() {
    @Override public void process(INPUT tuple)
    {
      OUTPUT outputTuple = convert(tuple);
      outputPort.emit(outputTuple);
    }
  };
}
