package com.datatorrent.demos.adsdimension.generic;

import com.datatorrent.api.LocalMode;
import com.datatorrent.demos.adsdimension.Application;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class DimensionOperatorBenchmarkTest
{
  @Test
  public void test()
  {
    DimensionOperatorBenchmark app = new DimensionOperatorBenchmark();
    LocalMode lma = LocalMode.newInstance();
    app.populateDAG(lma.getDAG(), new Configuration(false));
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run(10000);
  }
}
