/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.adsdimension;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.contrib.hds.tfile.TFileImpl;
import com.datatorrent.demos.adsdimension.HDSMapQueryOperator.HDSRangeQueryResult;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.google.common.util.concurrent.MoreExecutors;

public class HDSMapQueryOperatorTest
{
  @Rule
  public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  @Test
  public void testQuery() throws Exception {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    HDSMapQueryOperator hdsOut = new HDSMapQueryOperator() {
      @Override
      public void setup(OperatorContext arg0)
      {
        super.setup(arg0);
        super.writeExecutor = super.queryExecutor = MoreExecutors.sameThreadExecutor(); // synchronous processing
      }
    };
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsOut.setFileStore(hdsFile);
    hdsFile.setBasePath(testInfo.getDir());
    EventDescription eventDesc = GenericEventSerializerTest.getDataDesc();
    MapAggregator aggregator = new MapAggregator(eventDesc);
    aggregator.init("time=MINUTES:pubId:adId:adUnit");
    GenericEventSerializer serializer = new GenericEventSerializer(eventDesc);
    hdsOut.setEventDescriptor(eventDesc);
    hdsOut.setSerialiser(serializer);
    hdsOut.setAggregator(aggregator);
    hdsOut.setMaxCacheSize(1);
    hdsOut.setFlushIntervalCount(0);
    hdsOut.setup(null);

    hdsOut.setDebug(false);

    CollectorTestSink<HDSMapQueryOperator.HDSRangeQueryResult> queryResults = new CollectorTestSink<HDSMapQueryOperator.HDSRangeQueryResult>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink) queryResults;
    hdsOut.queryResult.setSink(tmp);

    hdsOut.beginWindow(1);

    long baseTime = System.currentTimeMillis();
    long baseMinute = TimeUnit.MILLISECONDS.convert(TimeUnit.MINUTES.convert(baseTime, TimeUnit.MILLISECONDS), TimeUnit.MINUTES);

    // Check aggregation for ae1 and ae2 as they have same key.
    MapAggregateEvent ae1 = new MapAggregateEvent(0);
    ae1.setTimestamp(baseMinute);
    ae1.keys.put("pubId", 1);
    ae1.keys.put("adId", 2);
    ae1.keys.put("adUnit", 3);
    ae1.fields.put("clicks", 10L);
    hdsOut.input.process(ae1);

    MapAggregateEvent ae2 = new MapAggregateEvent(0);
    ae2.setTimestamp(baseMinute);
    ae2.keys.put("pubId", 1);
    ae2.keys.put("adId", 2);
    ae2.keys.put("adUnit", 3);
    ae2.fields.put("clicks", 20L);
    hdsOut.input.process(ae2);

    MapAggregateEvent ae3 = new MapAggregateEvent(0);
    ae3.setTimestamp(baseMinute + TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES));
    ae3.keys.put("pubId", 1);
    ae3.keys.put("adId", 2);
    ae3.keys.put("adUnit", 3);
    ae3.fields.put("clicks", 10L);
    hdsOut.input.process(ae3);

    hdsOut.endWindow();

    hdsOut.beginWindow(2);

    JSONObject keys = new JSONObject();
    keys.put("pubId", 1);
    keys.put("adId", 2);
    keys.put("adUnit", 3);

    JSONObject query = new JSONObject();
    query.put("numResults", "20");
    query.put("keys", keys);
    query.put("id", "query1");
    query.put("startTime", baseMinute);
    query.put("endTime", baseMinute + TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES));

    hdsOut.query.process(query.toString());

    Assert.assertEquals("rangeQueries " + hdsOut.rangeQueries, 1, hdsOut.rangeQueries.size());
    HDSMapQueryOperator.HDSRangeQuery aq = hdsOut.rangeQueries.values().iterator().next();
    Assert.assertEquals("numTimeUnits " + hdsOut.rangeQueries, baseMinute, aq.startTime);

    hdsOut.endWindow();

    Assert.assertEquals("queryResults " + queryResults.collectedTuples, 1, queryResults.collectedTuples.size());
    HDSRangeQueryResult r = queryResults.collectedTuples.iterator().next();
    Assert.assertEquals("result points " + r, 2, r.data.size());

    // ae1 object is stored as referenced in cache, and when new tuple is aggregated,
    // the new values are updated in ae1 itself, causing following check to fail.
    //Assert.assertEquals("clicks", ae1.clicks + ae2.clicks, r.data.get(0).clicks);
    Assert.assertEquals("clicks", 30L, r.data.get(0).fields.get("clicks"));
    Assert.assertEquals("clicks", ae3.fields.get("clicks"), r.data.get(1).fields.get("clicks"));

    Assert.assertNotSame("deserialized", ae1, r.data.get(1));
    Assert.assertSame("from cache", ae3, r.data.get(1));

  }

}


