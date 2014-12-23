/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.samples.lib.math;

import com.datatorrent.api.LocalMode;
import com.datatorrent.samples.lib.math.PartitionMathSumSample;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class PartitionMathSumSampleTest
{
  @Test
  public void testApp()
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration();

    try {
      lma.prepareDAG(new PartitionMathSumSample(), conf);
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run(50);
  }
}
