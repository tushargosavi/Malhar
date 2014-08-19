package com.datatorrent.contrib.hds;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultWalManager
{
  HDSFileAccess bfs;
  Map<Long, BucketWalWriter> writers = Maps.newTreeMap();
  HDS.BucketManager store;

  long maxWalFileSize = 5 * 1024 * 1024;

  public long getMaxWalFileSize()
  {
    return maxWalFileSize;
  }

  public void setMaxWalFileSize(long maxWalFileSize)
  {
    this.maxWalFileSize = maxWalFileSize;
  }

  public DefaultWalManager(HDSFileAccess bfs, HDS.BucketManager store ) {
    this.bfs = bfs;
    this.store = store;
  }

  public void writeData(long bucketKey, MutableKeyValue data) throws IOException
  {
    BucketWalWriter writer = writers.get(bucketKey);

    if (writer == null) {
      // Initiate a new WAL for bucket, and run recovery if needed.
      BucketWalWriter w = new BucketWalWriter<MutableKeyValue>(bfs, bucketKey);
      w.setMaxWalFileSize(maxWalFileSize);
      w.setup();

      // get last committed LSN from store, and use that for recovery.
      if (store != null) {
        w.runRecovery(store, store.getRecoveryLSN(bucketKey));
      }

      writer = w;
      writers.put(bucketKey, writer);
    }
    writer.writeData(data);
  }


  public void endWindow(long wid) throws IOException
  {
    logger.info("======= EndWindow called {}", wid);
    for(BucketWalWriter writer : writers.values())
      writer.endWindow(wid);
  }

  /* Save metadata for each writter */
  public void saveMeta() throws IOException
  {
    for(BucketWalWriter writer : writers.values())
      writer.saveMeta();
  }

  public long getCommitedLSN(long bucketKey) {
    BucketWalWriter writer = writers.get(bucketKey);
    if (writer == null)
      return 0;
    return writer.getCommittedLSN();
  }

  public void teardown() throws IOException
  {
    for(BucketWalWriter writer : writers.values())
      writer.teardown();
  }

  private static transient final Logger logger = LoggerFactory.getLogger(DefaultWalManager.class);
}
