package com.datatorrent.demos.dimensions.benchmark;

import com.datatorrent.api.LocalMode;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class BenckMarkTest
{
  @Test
  public void test() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf =new Configuration(false);
    conf.addResource("benchmark-site.xml");
    lma.prepareDAG(new HDSWalBenchmarkApplication(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.run(10000);
  }
}
