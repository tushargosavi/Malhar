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
package com.datatorrent.demos.adsdimension;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.contrib.hds.tfile.TFileImpl;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.codehaus.jettison.json.JSONObject;
import org.junit.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class HDSApplicationTestMultiPart
{

  @Rule
  public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  @Test
  public void testPartitioning() throws Exception
  {
    int numPartitions = 4;
    File file = new File(testInfo.getDir());

    FileUtils.deleteDirectory(file);

    HDSQueryOperator hdsOut = new HDSQueryOperator();
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsOut.setFileStore(hdsFile);
    hdsFile.setBasePath("hdfs://localhost:9000/user/tushar/datatorrent/DB/");
    hdsOut.setAggregator(new AdInfo.AdInfoAggregator());
    hdsOut.setMaxCacheSize(1);
    hdsOut.setFlushIntervalCount(0);
    hdsOut.setup(null);
    hdsOut.setDebug(true);

    List<Partitioner.Partition<HDSOutputOperator>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<HDSOutputOperator>(hdsOut));
    Collection<Partitioner.Partition<HDSOutputOperator>> newPartitions = hdsOut.definePartitions(partitions, numPartitions - 1);
    Assert.assertEquals(numPartitions, newPartitions.size());
    hdsOut = null;

    // add data into partitions.
    CollectorTestSink<Object>[] sinks = new CollectorTestSink[numPartitions];
    HDSQueryOperator[] hdsParts = new HDSQueryOperator[numPartitions];
    int i = 0;
    for (Partitioner.Partition<HDSOutputOperator> p : newPartitions) {
      Assert.assertNotSame(hdsOut, p.getPartitionedInstance());
      hdsParts[i] = (HDSQueryOperator)p.getPartitionedInstance();
      hdsParts[i].setup(null);
      CollectorTestSink<HDSQueryOperator.HDSRangeQueryResult> queryResults = new CollectorTestSink<HDSQueryOperator.HDSRangeQueryResult>();
      sinks[i] = (CollectorTestSink) queryResults;
      hdsParts[i].queryResult.setSink(sinks[i]);
      i++;
    }

    long baseTime = System.currentTimeMillis();
    long baseMinute = TimeUnit.MILLISECONDS.convert(TimeUnit.MINUTES.convert(baseTime, TimeUnit.MILLISECONDS), TimeUnit.MINUTES);

    int numMinutes = 10;
    int w;
    for(w = 0; w < numMinutes; w++)
    {
      for(int p = 0; p < numPartitions; p++) {
        hdsParts[p].beginWindow(w+1);
        // Check aggregation for ae1 and ae2 as they have same key.
        AdInfo.AdInfoAggregateEvent ae1 = new AdInfo.AdInfoAggregateEvent();
        ae1.publisherId = p + 1;
        ae1.advertiserId = 0;
        ae1.adUnit = 0;
        ae1.aggregatorIndex = 3;
        ae1.timestamp = baseMinute + TimeUnit.MINUTES.toMillis(w);
        ae1.clicks = 10;
        hdsParts[p].input.process(ae1);
        hdsParts[p].endWindow();
      }
    }



    // Lets data be flushed to disks.
    for(i = 0; i < 10; i++) {
      for (int p = 0; p < numPartitions; p++) {
        hdsParts[p].beginWindow(w+1);
        hdsParts[p].endWindow();
      }
      w++;
    }

    Thread.sleep(5000);
    for (int p = 0; p < numPartitions; p++) {
      hdsParts[p].teardown();
    }

    /*
    for(int p = 0; p < numPartitions; p++) {
      hdsParts[p].beginWindow(w+1);
      JSONObject keys = new JSONObject();
      keys.put("publisherId", String.valueOf(3));

      JSONObject query = new JSONObject();
      query.put("numResults", "20");
      query.put("keys", keys);
      query.put("id", "query1");
      query.put("endTime", baseMinute + TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES));

      if (p == 0)
        hdsParts[p].query.process(query.toString());

      //Assert.assertEquals("rangeQueries " + hdsParts[p].rangeQueries, 1, hdsParts[p].rangeQueries.size());
      hdsParts[p].endWindow();
    }

    Thread.sleep(1000);



    for(i = 0; i < numPartitions; i++) {
      System.out.println("============= Form Partition " + i + " ========================== ");
      System.out.println("Query results " + sinks[i].collectedTuples.size());
      Iterator iter = sinks[i].collectedTuples.iterator();
      while(iter.hasNext()) {
        HDSQueryOperator.HDSRangeQueryResult r = (HDSQueryOperator.HDSRangeQueryResult)iter.next();
        System.out.println("data size " + r.data.size());
        for(AdInfo.AdInfoAggregateEvent agr : r.data)
        {
          System.out.println(agr);
        }
      }
    }
    */
  }

  @Test
  public void processQueries() throws Exception
  {

    int numPartitions = 4;
    File file = new File(testInfo.getDir());

    HDSQueryOperator hdsOut = new HDSQueryOperator();
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsOut.setFileStore(hdsFile);
    hdsFile.setBasePath("hdfs://localhost:9000/user/tushar/datatorrent/DB/");
    //hdsFile.setBasePath(testInfo.getDir());
    hdsOut.setAggregator(new AdInfo.AdInfoAggregator());
    hdsOut.setMaxCacheSize(1);
    hdsOut.setFlushIntervalCount(0);
    hdsOut.setup(null);
    hdsOut.setDebug(true);

    List<Partitioner.Partition<HDSOutputOperator>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<HDSOutputOperator>(hdsOut));
    Collection<Partitioner.Partition<HDSOutputOperator>> newPartitions = hdsOut.definePartitions(partitions, numPartitions - 1);
    Assert.assertEquals(numPartitions, newPartitions.size());
    hdsOut = null;

    // add data into partitions.
    CollectorTestSink<Object>[] sinks = new CollectorTestSink[numPartitions];
    HDSQueryOperator[] hdsParts = new HDSQueryOperator[numPartitions];
    int i = 0;
    for (Partitioner.Partition<HDSOutputOperator> p : newPartitions) {
      Assert.assertNotSame(hdsOut, p.getPartitionedInstance());
      hdsParts[i] = (HDSQueryOperator)p.getPartitionedInstance();
      hdsParts[i].setup(null);
      CollectorTestSink<HDSQueryOperator.HDSRangeQueryResult> queryResults = new CollectorTestSink<HDSQueryOperator.HDSRangeQueryResult>();
      sinks[i] = (CollectorTestSink) queryResults;
      hdsParts[i].queryResult.setSink(sinks[i]);
      i++;
    }

    long baseTime = System.currentTimeMillis();
    long baseMinute = TimeUnit.MILLISECONDS.convert(TimeUnit.MINUTES.convert(baseTime, TimeUnit.MILLISECONDS), TimeUnit.MINUTES);
    int w = 0;
    for(int p = 0; p < numPartitions; p++) {
      hdsParts[p].beginWindow(w+1);
      JSONObject keys = new JSONObject();
      keys.put("publisherId", String.valueOf(1));

      JSONObject query = new JSONObject();
      query.put("numResults", "20");
      query.put("keys", keys);
      query.put("id", "query1");
      query.put("endTime", baseMinute + TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES));


      hdsParts[p].query.process(query.toString());

      //Assert.assertEquals("rangeQueries " + hdsParts[p].rangeQueries, 1, hdsParts[p].rangeQueries.size());
      hdsParts[p].endWindow();
    }

    Thread.sleep(1000);



    for(i = 0; i < numPartitions; i++) {
      System.out.println("============= Form Partition " + i + " ========================== ");
      System.out.println("Query results " + sinks[i].collectedTuples.size());
      Iterator iter = sinks[i].collectedTuples.iterator();
      while(iter.hasNext()) {
        HDSQueryOperator.HDSRangeQueryResult r = (HDSQueryOperator.HDSRangeQueryResult)iter.next();
        System.out.println("data size " + r.data.size());
        for(AdInfo.AdInfoAggregateEvent agr : r.data)
        {
          System.out.println(agr);
        }
      }
    }
  }

}