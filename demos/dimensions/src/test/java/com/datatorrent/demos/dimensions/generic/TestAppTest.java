package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.LocalMode;
import org.apache.hadoop.conf.Configuration;


import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datatorrent.api.LocalMode;

public class TestAppTest
{
  @Test
  public void test() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    lma.prepareDAG(new TestAppMySql(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.run(10000);
  }

}
