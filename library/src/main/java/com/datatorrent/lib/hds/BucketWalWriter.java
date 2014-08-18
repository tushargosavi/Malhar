package com.datatorrent.lib.hds;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/* Manages WAL for a bucket */
public class BucketWalWriter<T extends BaseKeyVal>
{

  private static final String WAL_FILE_PREFIX = "WAL-";
  private static final String WALMETA_FILE_PREFIX = "WALMETA";
  private static final String WALMETA_TMP_FILE = WALMETA_FILE_PREFIX + ".tmp";

  static class OffsetRange {
    long start;
    long end;
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
  }

  transient public TreeMap<Long, WalFileMeta> files = Maps.newTreeMap();

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
  transient WALWriter<T> writer;

  transient private long bucketKey;

  /* current active WAL file id, it is read from WAL meta on startup */
  private int walFileId = -1;

  /* Last commited LSN on disk */
  private long committedLsn = -1;

  /* Current WAL size */
  private long commitedLength = 0;

  public BucketWalWriter(HDSFileAccess bfs, long bucketKey) {
    this.bfs = bfs;
    this.bucketKey = bucketKey;
  }

  /* Run recovery for bucket,
   * reads WAL and populate store uncommitedState.
   */
  public void runRecovery(HDS.BucketManager store, long id) throws IOException
  {
    if (store == null)
      return;

    /* Find WAL file id and offset where this window is located */
    WalFileMeta startWalFile = null;
    long offset = 0;
    for(Map.Entry<Long, WalFileMeta> fmeta : files.entrySet()) {
      boolean found = false;
      for(Map.Entry<Long, OffsetRange> w : fmeta.getValue().windowIndex.entrySet()) {
        OffsetRange range = w.getValue();
        if (range.start <= id && range.end >= id)
        {
          startWalFile = fmeta.getValue();
          offset = startWalFile.windowIndex.get(id).start;
          found = true;
          break;
        }
      }
      if (found)
        break;
    }

    if (startWalFile == null) {
      // Something wrong, requested sequence id not found in WAL.
      return;
    }

    long walid = startWalFile.walId;

    for (long i = walid; i < walFileId; i++) {
      WALReader<BaseKeyVal> wReader = new HDFSWalReader<BaseKeyVal>(bfs, bucketKey, i, null);
      wReader.seek(offset);
      offset = 0;

      while (wReader.hasNext()) {
        BaseKeyVal o = wReader.readNext();
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


    /* copy in block of 64 */
    long offset = fileMeta.committedBytes;
    int blockSize = 64 * 1024;
    long len;
    byte[] data = new byte[blockSize];
    DataInputStream in = bfs.getInputStream(bucketKey, WAL_FILE_PREFIX + fileMeta.walId);
    DataOutputStream out = bfs.getOutputStream(bucketKey, WAL_FILE_PREFIX + fileMeta.walId + "-recovery");

    while (offset > 0)
    {
      len = offset > blockSize? blockSize : offset;
      offset -= len;
      int count = in.read(data, 0, (int)len);
      out.write(data, 0, count);
    }

    in.close();
    out.close();

    bfs.rename(bucketKey, WAL_FILE_PREFIX + fileMeta.walId + "-recovery", WAL_FILE_PREFIX + fileMeta.walId);
  }

  public void writeData(T data ) throws IOException
  {
    if (writer == null)
      writer = new HDFSWalWritter<T>(bfs, bucketKey, ++walFileId, BaseKeyVal.DEFAULT_SERIALIZER);

    writer.append(data);
    if (maxUnflushedBytes > 0 && writer.unflushedCount() > maxUnflushedBytes)
      writer.flush();
  }

  /* Increase Log LSN for next window */
  public void beginWindow(long wid) {
    committedLsn++;
  }


  /* Update WAL meta data after committing window id wid */
  public void upadateWalMeta(long wid) {
    WalFileMeta fileMeta = files.get(walFileId);
    if (fileMeta == null) {
      fileMeta = new WalFileMeta();
      fileMeta.walId = walFileId;
      fileMeta.walSequenceStart = committedLsn;
      files.put((long)walFileId, fileMeta);
    }

    OffsetRange range = new OffsetRange(commitedLength, writer.logSize());
    fileMeta.windowIndex.put(committedLsn, range);
    fileMeta.walSequenceEnd = committedLsn;
    commitedLength = writer.logSize();
  }

  /* batch writes, and wait till file is written */
  public void endWindow(long wid) throws IOException
  {
    if (writer != null) {
      writer.flush();
    }

    upadateWalMeta(wid);

    /* Roll over log, if we have crossed the log size */
    if (maxWalFileSize > 0 && writer.logSize() > maxWalFileSize) {
      logger.info("Rolling over log {}", writer);
      writer.close();
      walFileId++;
      writer = new HDFSWalWritter(bfs, bucketKey, walFileId, BaseKeyVal.DEFAULT_SERIALIZER);
      commitedLength = 0;
    }
  }

  private static transient final Logger logger = LoggerFactory.getLogger(BucketWalWriter.class);

  /* Save WAL metadata in file */
  public void saveMeta() throws IOException
  {
    Kryo kryo = new Kryo();
    DataOutputStream out = bfs.getOutputStream(bucketKey, WALMETA_TMP_FILE);
    Output kout = new Output(out);
    kout.writeInt(walFileId);
    kout.writeLong(committedLsn);
    kryo.writeClassAndObject(kout, files);
    kout.close();
    out.close();

    bfs.rename(bucketKey, WALMETA_TMP_FILE, WALMETA_FILE_PREFIX);
  }

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

  void readMeta() {
    try {
      DataInputStream fin = bfs.getInputStream(bucketKey, WALMETA_FILE_PREFIX);
      Kryo kryo = new Kryo();
      Input in = new Input(fin);
      walFileId = in.readInt();
      committedLsn = in.readLong();
      files = (TreeMap)kryo.readClassAndObject(in);
    } catch (IOException ex) {
      walFileId = 0;
      committedLsn = 0;
      files = Maps.newTreeMap();
    }
    logger.info("Read metadata walFileId {} committedLsn {}", walFileId, committedLsn);
  }

  void setup() throws IOException
  {
    /* Restore stored metadata */
    readMeta();
    /* Restore last WAL file */
    restoreLastWal();
    /* open current WAL for writting, If file is present then WAL is opened in append mode,
     * else it is created. */
    writer = new HDFSWalWritter<T>(bfs, bucketKey, walFileId, BaseKeyVal.DEFAULT_SERIALIZER);
  }
  
  public long getCommittedLSN() {
    return committedLsn;
  }

}

