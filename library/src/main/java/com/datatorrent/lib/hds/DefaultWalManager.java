package com.datatorrent.lib.hds;

import com.datatorrent.lib.bucket.BucketStore;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultWalManager
{
  HDSFileAccess bfs;
  Map<Long, BucketWalWriter> writers = Maps.newTreeMap();
  HDS.BucketManager store;

  public DefaultWalManager(HDSFileAccess bfs, HDS.BucketManager store ) {
    this.bfs = bfs;
    this.store = store;
  }

  public void writeData(long bucketKey, BaseKeyVal data) throws IOException
  {
    BucketWalWriter writer = writers.get(bucketKey);

    if (writer == null) {
      // Initiate a new WAL for bucket, and run recovery if needed.
      BucketWalWriter w = new BucketWalWriter<BaseKeyVal>(bfs, bucketKey);
      w.setup();

      // TODO get last committed LSN from store, and use that for recovery.
      w.runRecovery(store, 0);
      writer = w;
      writers.put(bucketKey, writer);
    }
    // logger.info("Writting files {}", bucketKey);
    writer.writeData(data);
  }

  public void writeData(BaseKeyVal data) throws IOException
  {
    long bucketKey = data.getBucketKey();
    writeData(bucketKey, data);
  }

  public void writeCollection(Collection<BaseKeyVal> col) throws IOException
  {
    for(BaseKeyVal item : col)
      writeData(item);
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

  private static transient final Logger logger = LoggerFactory.getLogger(DefaultWalManager.class);
}
