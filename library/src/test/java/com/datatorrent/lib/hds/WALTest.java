package com.datatorrent.lib.hds;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.deser.ValueInstantiators;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class WALTest
{

  final String APP_BASE_PATH = "file:///home/tushar/work/hds/data/";
  final String DEFAULT_PATH_STR = "file:///home/tushar/work/hds/data/testwal";
  static final Random rand = new Random();

  static byte[] genRandomByteArray(int len) {
    byte[] val = new byte[len];
    rand.nextBytes(val);
    return val;
  }

  static BaseKeyVal genRandomTuple(int keyLen, int valLen) {
    byte[] key = genRandomByteArray(keyLen);
    byte[] val = genRandomByteArray(valLen);

    BaseKeyVal tuple = new BaseKeyVal(key, val);
    return tuple;
  }

  @Test
  public void testWalWriteAndRead() throws IOException
  {
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.support.append", true);
    //fs = new org.apache.hadoop.fs.RawLocalFileSystem();
    FileSystem fs = FileSystem.get(new Path(DEFAULT_PATH_STR).toUri(), conf);
    fs.setVerifyChecksum(false);
    HDSFileAccess bfs = new HDSFileAccessFSImpl(fs, APP_BASE_PATH);
    fs.delete(new Path(APP_BASE_PATH), true);

    HDFSWalWritter<BaseKeyVal> wWriter = new HDFSWalWritter<BaseKeyVal>(bfs, 1, 0, BaseKeyVal.DEFAULT_SERIALIZER);
    int wrote = 40;
    for (int i = 0; i < wrote; i++) {
      wWriter.append(genRandomTuple(100, 100));
    }
    wWriter.close();


    HDFSWalReader<BaseKeyVal> wReader = new HDFSWalReader<BaseKeyVal>(bfs, 1, 0, BaseKeyVal.DEFAULT_SERIALIZER);
    int read = 0;
    while (wReader.hasNext()) {
      read++;
      wReader.readNext();
    }

    Assert.assertEquals("Write and read same number of tuples ", wrote, read);
  }

  @Test
  public void testWalSkip() throws IOException
  {
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.support.append", true);
    //fs = new org.apache.hadoop.fs.RawLocalFileSystem();
    FileSystem fs = FileSystem.get(new Path(DEFAULT_PATH_STR).toUri(), conf);
    fs.setVerifyChecksum(false);
    HDSFileAccess bfs = new HDSFileAccessFSImpl(fs, APP_BASE_PATH);
    fs.delete(new Path(APP_BASE_PATH), true);

    long offset = 0;

    HDFSWalWritter<BaseKeyVal> wWriter = new HDFSWalWritter<BaseKeyVal>(bfs, 1, 0, BaseKeyVal.DEFAULT_SERIALIZER);
    int wrote = 40;
    for (int i = 0; i < wrote; i++) {
      wWriter.append(genRandomTuple(100, 100));
      if (i == 19)
        offset = wWriter.logSize();
    }
    wWriter.close();


    HDFSWalReader<BaseKeyVal> wReader = new HDFSWalReader<BaseKeyVal>(bfs, 1, 0, BaseKeyVal.DEFAULT_SERIALIZER);
    wReader.seek(offset);
    int read = 0;
    while (wReader.hasNext()) {
      read++;
      wReader.readNext();
    }

    Assert.assertEquals("Number of tuples read after skipping", read, 20);
  }

  @Test
  public void testWalRolling() throws IOException
  {
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.support.append", true);
    //fs = new org.apache.hadoop.fs.RawLocalFileSystem();
    FileSystem fs = FileSystem.get(new Path(DEFAULT_PATH_STR).toUri(), conf);
    fs.setVerifyChecksum(false);
    HDSFileAccess bfs = new HDSFileAccessFSImpl(fs, APP_BASE_PATH);
    fs.delete(new Path(APP_BASE_PATH), true);

    DefaultWalManager mgr = new DefaultWalManager(bfs, null);
    mgr.setMaxWalFileSize(1024);

    mgr.writeData(1, genRandomTuple(500, 500));
    mgr.writeData(1, genRandomTuple(500, 500));

    mgr.endWindow(0);

    mgr.writeData(1, genRandomTuple(500, 500));
    mgr.writeData(1, genRandomTuple(500, 500));
    mgr.endWindow(1);

    boolean exists = fs.exists(new Path(APP_BASE_PATH + "1/WAL-1"));
    Assert.assertEquals("New Wal created ", exists, true);
  }

  static class MyBucketManager implements HDS.BucketManager {
    private int count;

    @Override public void put(long bucketKey, byte[] key, byte[] value) throws IOException
    {
      System.out.println("Adding key in store");
      count ++;
    }

    @Override public byte[] get(long bucketKey, byte[] key) throws IOException
    {
      return new byte[0];
    }

    public int getCount() {
      return count;
    }

    @Override public long getRecoveryLSN(long bucketKey)
    {
      return 1;
    }
  }

  @Test
  public void testWalRecovery() throws IOException
  {
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.support.append", true);
    //fs = new org.apache.hadoop.fs.RawLocalFileSystem();
    FileSystem fs = FileSystem.get(new Path(DEFAULT_PATH_STR).toUri(), conf);
    fs.setVerifyChecksum(false);
    HDSFileAccess bfs = new HDSFileAccessFSImpl(fs, APP_BASE_PATH);
    fs.delete(new Path(APP_BASE_PATH), true);

    DefaultWalManager mgr = new DefaultWalManager(bfs, null);
    mgr.setMaxWalFileSize(1024 * 1024);


    mgr.writeData(1, genRandomTuple(500, 500));
    mgr.writeData(1, genRandomTuple(500, 500));
    mgr.endWindow(0);

    mgr.writeData(1, genRandomTuple(500, 500));
    mgr.writeData(1, genRandomTuple(500, 500));
    mgr.endWindow(1);

    // Checkpoint WAL state.
    mgr.saveMeta();

    mgr.writeData(1, genRandomTuple(500, 500));
    mgr.writeData(1, genRandomTuple(500, 500));

    MyBucketManager myStore = new MyBucketManager();

    mgr = new DefaultWalManager(bfs, myStore);
    // This should run recovery, as first tuple is added in bucket
    mgr.writeData(1, genRandomTuple(100, 100));

    System.out.println("number of tuples in store is " + myStore.getCount());

  }
}
