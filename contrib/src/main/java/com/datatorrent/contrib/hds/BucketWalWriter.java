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
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/* Manages WAL for a bucket */
public class BucketWalWriter
{
  public static final String WAL_FILE_PREFIX = "_WAL-";
  public static final String WALMETA_FILE_PREFIX = "_WALMETA";
  public static final String WALMETA_TMP_FILE = WALMETA_FILE_PREFIX + ".tmp";

  static class OffsetRange {
    long start;
    long end;

    private OffsetRange() {}

    OffsetRange(long start, long end) {
      this.start = start;
      this.end = end;
    }
  }

  /* Data to be maintain for each file */
  private static class WalFileMeta implements Comparable<WalFileMeta> {
    public long walId;
    public long walSequenceStart;
    public long walSequenceEnd;
    public long committedBytes;
    public TreeMap<Long, OffsetRange> windowIndex = Maps.newTreeMap();

    @Override public int compareTo(WalFileMeta o)
    {
      return (int)(walId - o.walId);
    }

    @Override public String toString()
    {
      return "WalFileMeta{" +
          "walId=" + walId +
          ", walSequenceStart=" + walSequenceStart +
          ", walSequenceEnd=" + walSequenceEnd +
          ", committedBytes=" + committedBytes +
          '}';
    }
  }

  public TreeMap<Long, WalFileMeta> files = Maps.newTreeMap();

  /* Backend Filesystem managing WAL */
  transient HDSFileAccess bfs;

  /*
   * If maxmimum number of bytes allowed to be written to file between flush,
   * default is 64K.
   */
  transient long maxUnflushedBytes = 64 * 1024;

  /* Maximum number of bytes per WAL file,
   * default is 100M */
  transient long maxWalFileSize = 512 * 1024;

  /* Replication count for WAL log */
  transient  int replication = 3;


  /* The class responsible writing WAL entry to file */
  transient WALWriter writer;

  transient private long bucketKey;

  /* current active WAL file id, it is read from WAL meta on startup */
  private long walFileId = -1;

  /* Last commited LSN on disk */
  private long committedLsn = -1;

  /* current processing LSN window */
  private transient long lsn = 0;

  /* Current WAL size */
  private long commitedLength = 0;

  private BucketWalWriter() { }

  public BucketWalWriter(HDSFileAccess bfs, long bucketKey) {
    this.bfs = bfs;
    this.bucketKey = bucketKey;
  }

  public void setFileStore(HDSFileAccess bfs) {
    this.bfs = bfs;
  }

  /* Run recovery for bucket,
   * reads WAL and populate store uncommittedState.
   */
  public void runRecovery(HDS.BucketManager store, long id) throws IOException
  {
    logger.info("Recovering last processed LSN is " + id);
    if (store == null)
      return;

    /* Find WAL file id and offset where this window is located */
    WalFileMeta startWalFile = null;
    long offset = 0;
    for(Map.Entry<Long, WalFileMeta> fmetaEntry : files.entrySet()) {
      WalFileMeta fmeta =  fmetaEntry.getValue();
      logger.info("Range for file " + fmeta.walId + " is (" + fmeta.walSequenceStart + ", " + fmeta.walSequenceEnd + ")");
      if (fmeta.walSequenceStart <= id && fmeta.walSequenceEnd >= id)
      {
          // This file contains, sequence id
          startWalFile = fmeta;
          long key = startWalFile.windowIndex.ceilingKey(id);
          offset = startWalFile.windowIndex.get(key).end;
          break;
      }
    }

    if (startWalFile == null) {
      logger.warn("Sequence id not found in metadata bucket {} lsn {}", bucketKey, id);
      return;
    }

    long walid = startWalFile.walId;
    logger.info("Recovering starting from file " + walid + " offset " + offset + " till " + walFileId);
    for (long i = walid; i < walFileId; i++) {
      WALReader wReader = new HDFSWalReader(bfs, bucketKey, WAL_FILE_PREFIX + i);
      wReader.seek(offset);
      offset = 0;

      while (wReader.advance()) {
        MutableKeyValue o = wReader.get();
        logger.info("Adding data during recovery");
        store.put(bucketKey, o.getKey(), o.getValue());
      }

      wReader.close();
    }
  }

  /* Last know committed offset, is stored in metadata, when failure occures while we were in
     middle of
   */
  private void restoreLastWal() throws IOException
  {
    /* Get the metadata for last WAL for this bucket */
    Map.Entry<Long, WalFileMeta> lastEntry = files.lastEntry();
    if (lastEntry == null)
      return;
    WalFileMeta fileMeta = lastEntry.getValue();

    /* No WAL entries */
    if (fileMeta == null)
      return;

    /* no data to commit */
    if (fileMeta.committedBytes == 0)
      return;

    bfs.truncate(bucketKey, WAL_FILE_PREFIX + fileMeta.walId, fileMeta.committedBytes);
  }

  public long append(byte[] key, byte[] value) throws IOException
  {
    if (writer == null)
      writer = new HDFSWalWriter(bfs, bucketKey, WAL_FILE_PREFIX + walFileId);

    writer.append(key, value);
    if (maxUnflushedBytes > 0 && writer.getUnflushedCount() > maxUnflushedBytes)
      writer.flush();
    return lsn;
  }

  public void setLSN(long lsn) {
    committedLsn = lsn;
  }

  /* Update WAL meta data after committing window id wid */
  public void upadateWalMeta() {
    WalFileMeta fileMeta = files.get(walFileId);
    if (fileMeta == null) {
      fileMeta = new WalFileMeta();
      fileMeta.walId = walFileId;
      fileMeta.walSequenceStart = committedLsn;
      files.put((long)walFileId, fileMeta);
    }

    logger.info("file " + walFileId + "lsn " + committedLsn + " start " + commitedLength + " end " + writer.logSize());
    OffsetRange range = new OffsetRange(commitedLength, writer.logSize());
    fileMeta.windowIndex.put(committedLsn, range);
    fileMeta.walSequenceEnd = committedLsn;
    commitedLength = writer.logSize();
    fileMeta.committedBytes = commitedLength;
  }

  /* batch writes, and wait till file is written */
  public void endWindow() throws IOException
  {
    if (writer != null) {
      writer.flush();
    }

    committedLsn = lsn;
    lsn++;

    upadateWalMeta();

    /* Roll over log, if we have crossed the log size */
    if (maxWalFileSize > 0 && writer.logSize() > maxWalFileSize) {
      logger.info("Rolling over log {}", writer);
      writer.close();
      walFileId++;
      writer = null;
      commitedLength = 0;
    }
  }

  private static transient final Logger logger = LoggerFactory.getLogger(BucketWalWriter.class);

  /* Remove old WAL files, and their metadata */
  public void cleanup(long cleanLsn) throws IOException
  {
    for (WalFileMeta file : files.values()) {

      /* Do not touch current WAL file */
      if (file.walId == walFileId)
        continue;

      /* Remove old file from WAL */
      if (file.walSequenceStart < cleanLsn) {
        files.remove(file);
        bfs.delete(bucketKey, "WAL-" + file.walId);
      }
    }
  }

  public long getMaxWalFileSize()
  {
    return maxWalFileSize;
  }

  public void setMaxWalFileSize(long maxWalFileSize)
  {
    this.maxWalFileSize = maxWalFileSize;
  }

  public int getReplication()
  {
    return replication;
  }

  public void setReplication(int replication)
  {
    this.replication = replication;
  }

  public long getMaxUnflushedBytes()
  {
    return maxUnflushedBytes;
  }

  public void setMaxUnflushedBytes(long maxUnflushedBytes)
  {
    this.maxUnflushedBytes = maxUnflushedBytes;
  }

  /* Save WAL metadata in file */
  public void saveMeta() throws IOException
  {
    logger.info("Saving WAL metadata for bucket " + bucketKey + " commitedLSN " + committedLsn);
    printMeta(0);
    Kryo kryo = new Kryo();
    DataOutputStream out = bfs.getOutputStream(bucketKey, WALMETA_TMP_FILE);
    Output kout = new Output(out);
    kout.writeLong(walFileId);
    kout.writeLong(committedLsn);
    kryo.writeClassAndObject(kout, files);

    kout.close();
    out.close();

    bfs.rename(bucketKey, WALMETA_TMP_FILE, WALMETA_FILE_PREFIX);
  }

  /* For debugging purpose */
  private void printMeta(int id)
  {
    for(WalFileMeta meta : files.values()) {
      logger.debug("{} FileMeata {}", id, meta);
      for(Map.Entry<Long, OffsetRange> widx : meta.windowIndex.entrySet())
        logger.debug(id + "read file " + meta.walId + " lsn " + widx.getKey() + " start " + widx.getValue().start + " end " + widx.getValue().end);
    }
  }

  void readMeta() {
    try {
      DataInputStream fin = bfs.getInputStream(bucketKey, WALMETA_FILE_PREFIX);
      Kryo kryo = new Kryo();
      Input in = new Input(fin);
      walFileId = in.readLong();
      committedLsn = in.readLong();
      lsn = committedLsn + 1;
      files = (TreeMap)kryo.readClassAndObject(in);
    } catch (IOException ex) {
      walFileId = -1;
      committedLsn = -1;
      lsn = 0;
      files = Maps.newTreeMap();
    }

    printMeta(1);
    logger.info("Read metadata walFileId {} committedLsn {}", walFileId, committedLsn);
  }

  void init() throws IOException
  {
    /* Restore last WAL file */
    restoreLastWal();
    /* open current WAL for writting, If file is present then WAL is opened in append mode,
     * else it is created. */
    writer = null;

    /* Use new file for fresh restart */
    walFileId++;
  }

  void setup() throws IOException
  {
    printMeta(1);

    /* Restore last WAL file */
    restoreLastWal();
    /* open current WAL for writting, If file is present then WAL is opened in append mode,
     * else it is created. */
    writer = null;

    /* Use new file for fresh restart */
    walFileId++;
  }


  public void teardown() throws IOException
  {
    writer.close();
  }

  public long getCommittedLSN() {
    return committedLsn;
  }

  void close() throws IOException
  {
    writer.close();
  }
}

