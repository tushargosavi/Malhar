package com.datatorrent.demos.adsdimension;

import com.datatorrent.api.DefaultInputPort;
import java.util.Map;

public class MapHDSOperator extends HDSOutputOperator
{

  DefaultInputPort<Map<String, Object>> in = new DefaultInputPort<Map<String, Object>>()
  {
    @Override public void process(Map<String, Object> tuple)
    {

    }
  };

  void processTuple(Map<String, String> tuple)
  {

  }
}
