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
package com.datatorrent.contrib.hds;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.google.common.util.concurrent.MoreExecutors;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class WALTest
{
  static final Random rand = new Random();

  File file = new File("target/hds");

  static byte[] genRandomByteArray(int len) {
    byte[] val = new byte[len];
    rand.nextBytes(val);
    return val;
  }

  @Test
  public void testWalWriteAndRead() throws IOException
  {
    FileUtils.deleteDirectory(file);
    HDSFileAccessFSImpl bfs = new HDSFileAccessFSImpl();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();

    int keySize = 100;
    int valSize = 100;
    int numTuples = 100;

    HDFSWalWriter wWriter = new HDFSWalWriter(bfs, 1, "WAL-0");
    for (int i = 0; i < numTuples; i++) {
      wWriter.append(genRandomByteArray(keySize), genRandomByteArray(valSize));
    }
    wWriter.close();

    File wal0 = new File(file.getAbsoluteFile().toString() + "/1/WAL-0");
    Assert.assertEquals("WAL file created ", true, wal0.exists());

    HDFSWalReader wReader = new HDFSWalReader(bfs, 1, "WAL-0");
    int read = 0;
    while (wReader.advance()) {
      read++;
      MutableKeyValue keyVal = wReader.get();
      Assert.assertEquals("Key size ", keySize, keyVal.getKey().length);
      Assert.assertEquals("Value size ", valSize, keyVal.getValue().length);
    }

    Assert.assertEquals("Write and read same number of tuples ", numTuples, read);
  }

  @Test
  public void testWalSkip() throws IOException
  {
    FileUtils.deleteDirectory(file);
    HDSFileAccessFSImpl bfs = new HDSFileAccessFSImpl();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();

    long offset = 0;

    HDFSWalWriter wWriter = new HDFSWalWriter(bfs, 1, "WAL-0");
    int totalTuples = 100;
    int recoveryTuples = 30;
    for (int i = 0; i < totalTuples; i++) {
      wWriter.append(genRandomByteArray(100), genRandomByteArray(100));
      if (i == recoveryTuples)
        offset = wWriter.logSize();
    }
    logger.info("total file size is " + wWriter.logSize() + " recovery offset is " + offset);
    wWriter.close();

    HDFSWalReader wReader = new HDFSWalReader(bfs, 1, "WAL-0");
    wReader.seek(offset);
    int read = 0;
    while (wReader.advance()) {
      read++;
      wReader.get();
    }

    Assert.assertEquals("Number of tuples read after skipping", read, (totalTuples - recoveryTuples - 1));
  }

  @Test
  public void testWalRolling() throws IOException
  {
    FileUtils.deleteDirectory(file);
    HDSFileAccessFSImpl bfs = new HDSFileAccessFSImpl();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();


    DefaultWalManager mgr = new DefaultWalManager(bfs, null);
    mgr.setMaxWalFileSize(1024);

    mgr.append(1, genRandomByteArray(500), genRandomByteArray(500));
    mgr.append(1, genRandomByteArray(500), genRandomByteArray(500));

    mgr.endWindow(0);

    mgr.append(1, genRandomByteArray(500), genRandomByteArray(500));
    mgr.append(1, genRandomByteArray(500), genRandomByteArray(500));
    mgr.endWindow(1);

    File wal0 = new File(file.getAbsoluteFile().toString() + "/1/_WAL-1");
    Assert.assertEquals("New Wal-0 created ", wal0.exists(), true);

    File wal1 = new File(file.getAbsoluteFile().toString() + "/1/_WAL-2");
    Assert.assertEquals("New Wal-1 created ", wal1.exists(), true);
  }

  static class MyBucketManager implements HDS.BucketManager {
    private int count;

    @Override public void put(long bucketKey, byte[] key, byte[] value) throws IOException
    {
      count ++;
    }

    @Override public byte[] get(long bucketKey, byte[] key) throws IOException
    {
      return new byte[0];
    }

    public int getCount() {
      return count;
    }
  }

  @Test
  public void testWalRecovery() throws IOException
  {
    File file = new File("target/hds");
    FileUtils.deleteDirectory(file);
    System.out.println("directory is " + file.getAbsolutePath());
    final long BUCKET1 = 1L;

    File bucket1Dir = new File(file, Long.toString(BUCKET1));
    File bucket1WalFile = new File(bucket1Dir, BucketWalWriter.WAL_FILE_PREFIX + 1);
    RegexFileFilter dataFileFilter = new RegexFileFilter("\\d+.*");

    FileUtils.deleteDirectory(file);
    HDSFileAccessFSImpl bfs = new HDSFileAccessFSImpl();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();

    HDSBucketManager hds = new HDSBucketManager();
    hds.setFileStore(bfs);
    hds.setKeyComparator(new HDSTest.MyDataKey.SequenceComparator());
    hds.setFlushIntervalCount(1);
    hds.setFlushSize(3);
    hds.setup(null);
    hds.writeExecutor = MoreExecutors.sameThreadExecutor();

    hds.beginWindow(1001);
    hds.put(1, genRandomByteArray(500), genRandomByteArray(500));
    hds.put(1, genRandomByteArray(500), genRandomByteArray(500));
    hds.endWindow();

    hds.beginWindow(1002);
    hds.put(1, genRandomByteArray(500), genRandomByteArray(500));
    hds.put(1, genRandomByteArray(500), genRandomByteArray(500));
    // Data files are written in this endWindow();
    hds.endWindow();

    hds.beginWindow(1003);
    hds.put(1, genRandomByteArray(500), genRandomByteArray(500));
    hds.put(1, genRandomByteArray(500), genRandomByteArray(500));
    // Wal state is saved in this window.
    hds.endWindow();

    hds.forceWal();

    hds.teardown();

    Kryo kryo = new Kryo();
    com.esotericsoftware.kryo.io.ByteBufferOutput oo = new ByteBufferOutput(100000);
    kryo.writeObject(oo, hds);
    oo.flush();
    com.esotericsoftware.kryo.io.ByteBufferInput oi = new ByteBufferInput(oo.getByteBuffer());
    HDSBucketManager newOperator = kryo.readObject(oi, HDSBucketManager.class);
    newOperator.setFileStore(bfs);
    newOperator.setKeyComparator(new HDSTest.MyDataKey.SequenceComparator());
    newOperator.setFlushIntervalCount(1);
    newOperator.setFlushSize(3);
    newOperator.setup(null);
    newOperator.writeExecutor = MoreExecutors.sameThreadExecutor();

    // This should run recovery, as first tuple is added in bucket
    newOperator.put(1, genRandomByteArray(500), genRandomByteArray(500));

    System.out.println("Unflushed data for bucket 1 is " + hds.unflushedData(1));
    Assert.assertEquals("Number of tuples in cache", 3, hds.unflushedData(1));

    //Assert.assertEquals("Number of tuples in store ", 2, mgr.buckets.get(1));getCount());
  }

  private static transient final Logger logger = LoggerFactory.getLogger(DefaultWalManager.class);

}
