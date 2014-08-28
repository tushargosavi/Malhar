package com.datatorrent.contrib.hds;

import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONObject;
import org.apache.hadoop.hbase.util.Base64;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class HDSJSonClientTest
{
  @Rule
  public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  /**
   * Test REST based client.
   */
  @Test
  public void testGet() throws Exception
  {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    Slice key = HDSTest.newKey(1, 1);
    String data = "data1";

    HDSFileAccessFSImpl fa = new MockFileAccess();
    fa.setBasePath(file.getAbsolutePath());
    HDSBucketManager hds = new HDSBucketManager();
    hds.setFileStore(fa);
    hds.setFlushSize(0); // flush after every key

    hds.setup(null);
    hds.writeExecutor = MoreExecutors.sameThreadExecutor(); // synchronous flush on endWindow
    hds.beginWindow(1);
    hds.put(HDSTest.getBucketKey(key), key.buffer, data.getBytes());
    hds.endWindow();
    hds.teardown();

    // get fresh instance w/o cached readers
    hds = TestUtils.clone(new Kryo(), hds);
    hds.setup(null);
    hds.beginWindow(1);
    byte[] val = hds.get(HDSTest.getBucketKey(key), key.buffer);
    hds.endWindow();
    hds.teardown();
    Assert.assertArrayEquals("get", data.getBytes(), val);

    HDSJSonClient hc = new HDSJSonClient();
    hc.setStore(hds);

    JSONObject query = new JSONObject();
    query.put("id", "query1");
    query.put("bucketKey", 1);
    query.put("key", Base64.encodeBytes(data.getBytes()));


    HDSQuery q = hc.registerQuery(query.toString());
    HDSQuery.QueryResult res = hc.processQuery(q);
    val = Base64.decode(res.value);
    Assert.assertArrayEquals("get", data.getBytes(), val);
  }

}
