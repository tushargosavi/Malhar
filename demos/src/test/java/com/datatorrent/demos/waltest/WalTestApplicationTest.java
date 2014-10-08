package com.datatorrent.demos.waltest;

import com.datatorrent.api.LocalMode;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class WalTestApplicationTest
{
  @Test
  public void testApplication() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);

    conf.set("dt.operator.HDSOut.prop.fileStore.basePath", "target/HDSApplicationTestStore");
    conf.set("dt.loggers.level", "server.*:INFO");

    lma.prepareDAG(new WalTestApplication(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();
  }

}

