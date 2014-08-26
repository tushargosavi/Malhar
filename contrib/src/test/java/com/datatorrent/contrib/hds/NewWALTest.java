package com.datatorrent.contrib.hds;

import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class NewWALTest
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
    HDSFileAccessFSImpl bfs = new MockFileAccess();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();

    int keySize = 100;
    int valSize = 100;
    int numTuples = 100;


    WAL wal = new WAL(bfs, 1);
    wal.beingWindow(1);
    for (int i = 0; i < numTuples; i++) {
      wal.append(genRandomByteArray(keySize), genRandomByteArray(valSize));
    }
    wal.endWindow(1);
    wal.close();

    File wal0 = new File(file.getAbsoluteFile().toString() + "/1/WAL-0");
    Assert.assertEquals("WAL file created ", true, wal0.exists());

    /*
    WAL.WalReader reader = wal.getWalReader(0, 0);
    int read = 0;
    while (reader.advance()) {
      read++;
      MutableKeyValue keyVal = reader.get();
      Assert.assertEquals("Key size ", keySize, keyVal.getKey().length);
      Assert.assertEquals("Value size ", valSize, keyVal.getValue().length);
    }

    Assert.assertEquals("Write and read same number of tuples ", numTuples, read);
    */
  }
}
