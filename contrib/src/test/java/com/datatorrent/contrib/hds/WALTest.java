package com.datatorrent.contrib.hds;

import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class WALTest
{


  final String APP_BASE_PATH = "file:///home/tushar/work/hds/data/";
  final String DEFAULT_PATH_STR = "file:///home/tushar/work/hds/data/testwal";
  static final Random rand = new Random();

  File file = new File("target/hds");

  static byte[] genRandomByteArray(int len) {
    byte[] val = new byte[len];
    rand.nextBytes(val);
    return val;
  }

  static MutableKeyValue genRandomTuple(int keyLen, int valLen) {
    byte[] key = genRandomByteArray(keyLen);
    byte[] val = genRandomByteArray(valLen);

    MutableKeyValue tuple = new MutableKeyValue(key, val);
    return tuple;
  }

  @Test
  public void testWalWriteAndRead() throws IOException
  {
    FileUtils.deleteDirectory(file);
    HDSFileAccessFSImpl bfs = new HDSFileAccessFSImpl();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();

    HDFSWalWritter<MutableKeyValue> wWriter = new HDFSWalWritter<MutableKeyValue>(bfs, 1, 0, MutableKeyValue.DEFAULT_SERIALIZER);
    int wrote = 40;
    for (int i = 0; i < wrote; i++) {
      wWriter.append(genRandomTuple(100, 100));
    }
    wWriter.close();


    HDFSWalReader<MutableKeyValue> wReader = new HDFSWalReader<MutableKeyValue>(bfs, 1, 0, MutableKeyValue.DEFAULT_SERIALIZER);
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
    FileUtils.deleteDirectory(file);
    HDSFileAccessFSImpl bfs = new HDSFileAccessFSImpl();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();

    long offset = 0;

    HDFSWalWritter<MutableKeyValue> wWriter = new HDFSWalWritter<MutableKeyValue>(bfs, 1, 0, MutableKeyValue.DEFAULT_SERIALIZER);
    int wrote = 40;
    for (int i = 0; i < wrote; i++) {
      wWriter.append(genRandomTuple(100, 100));
      if (i == 19)
        offset = wWriter.logSize();
    }
    wWriter.close();


    HDFSWalReader<MutableKeyValue> wReader = new HDFSWalReader<MutableKeyValue>(bfs, 1, 0, MutableKeyValue.DEFAULT_SERIALIZER);
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
    FileUtils.deleteDirectory(file);
    HDSFileAccessFSImpl bfs = new HDSFileAccessFSImpl();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();


    DefaultWalManager mgr = new DefaultWalManager(bfs, null);
    mgr.setMaxWalFileSize(1024);

    mgr.writeData(1, genRandomTuple(500, 500));
    mgr.writeData(1, genRandomTuple(500, 500));

    mgr.endWindow(0);

    mgr.writeData(1, genRandomTuple(500, 500));
    mgr.writeData(1, genRandomTuple(500, 500));
    mgr.endWindow(1);

    File walFile = new File(file.getAbsoluteFile().toString() + "/1/WAL-1");
    boolean exists = walFile.exists();
    Assert.assertEquals("New Wal created ", exists, true);
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

    @Override public long getRecoveryLSN(long bucketKey)
    {
      return 1;
    }
  }

  @Test
  public void testWalRecovery() throws IOException
  {
    FileUtils.deleteDirectory(file);
    HDSFileAccessFSImpl bfs = new HDSFileAccessFSImpl();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();

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
    mgr.teardown();

    MyBucketManager myStore = new MyBucketManager();

    mgr = new DefaultWalManager(bfs, myStore);
    // This should run recovery, as first tuple is added in bucket
    mgr.writeData(1, genRandomTuple(100, 100));

    Assert.assertEquals("Number of tuples in store ", 2, myStore.getCount());
  }
}
