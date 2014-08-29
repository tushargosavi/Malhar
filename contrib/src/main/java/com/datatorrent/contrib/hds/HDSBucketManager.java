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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.CheckpointListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hds.HDSFileAccess.HDSFileReader;
import com.datatorrent.contrib.hds.HDSFileAccess.HDSFileWriter;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Manager for buckets. Can be sub-classed as operator or used in composite pattern.
 */
public class HDSBucketManager implements HDS.BucketManager, CheckpointListener, Operator
{
  public static final String FNAME_WAL = "_WAL";
  public static final String FNAME_META = "_META";

  private final transient HashMap<Long, BucketMeta> metaCache = Maps.newHashMap();
  private long windowId;
  private transient long lastFlushWindowId;
  private final transient HashMap<Long, Bucket> buckets = Maps.newHashMap();
  private final transient Kryo kryo = new Kryo();
  @VisibleForTesting
  protected transient ExecutorService writeExecutor;
  private transient Exception writerError;

  @NotNull
  private Comparator<Slice> keyComparator = new DefaultKeyComparator();
  @Valid
  @NotNull
  private HDSFileAccess fileStore;
  private int maxFileSize = 64000;
  private int maxWalFileSize = 64 * 1024 * 1024;
  private int flushSize = 1000000;
  private int flushIntervalCount = 30;

  private HashMap<Long, WalMeta> walMeta = Maps.newHashMap();

  public HDSFileAccess getFileStore()
  {
    return fileStore;
  }

  public void setFileStore(HDSFileAccess fileStore)
  {
    this.fileStore = fileStore;
  }

  /**
   * Compare keys for sequencing as secondary level of organization within buckets.
   * In most cases it will be implemented using a time stamp as leading component.
   * @return
   */
  public Comparator<Slice> getKeyComparator()
  {
    return this.keyComparator;
  }

  public void setKeyComparator(Comparator<Slice> keyComparator)
  {
    this.keyComparator = keyComparator;
  }

  /**
   * Size limit for data files. Files are rolled once the limit has been exceeded.
   * The final size of a file can be larger than the limit by the size of the last/single entry written to it.
   * @return
   */
  public int getMaxFileSize()
  {
    return maxFileSize;
  }

  public void setMaxFileSize(int maxFileSize)
  {
    this.maxFileSize = maxFileSize;
  }

  /**
   * Size limit for WAL files. Files are rolled once the limit has been exceeded.
   * The final size of a file can be larger than the limit, as files are rolled at
   * end of the operator window.
   * @return
   */
  public int getMaxWalFileSize()
  {
    return maxWalFileSize;
  }

  public void setMaxWalFileSize(int maxWalFileSize)
  {
    this.maxWalFileSize = maxWalFileSize;
  }

  /**
   * The number of changes collected in memory before flushing to persistent storage.
   * @return
   */
  public int getFlushSize()
  {
    return flushSize;
  }

  public void setFlushSize(int flushSize)
  {
    this.flushSize = flushSize;
  }

  /**
   * Cached writes are flushed to persistent storage periodically. The interval is specified as count of windows and
   * establishes the maximum latency for changes to be written while below the {@link #flushSize} threshold.
   *
   * @return
   */
  @Min(value=1)
  public int getFlushIntervalCount()
  {
    return flushIntervalCount;
  }

  public void setFlushIntervalCount(int flushIntervalCount)
  {
    this.flushIntervalCount = flushIntervalCount;
  }

  public static byte[] asArray(Slice slice)
  {
    return Arrays.copyOfRange(slice.buffer, slice.offset, slice.offset + slice.length);
  }

  public static Slice toSlice(byte[] bytes)
  {
    return new Slice(bytes, 0, bytes.length);
  }

  /**
   * Write data to size based rolling files
   * @param bucket
   * @param bucketMeta
   * @param data
   * @throws IOException
   */
  private void writeFile(Bucket bucket, BucketMeta bucketMeta, TreeMap<Slice, byte[]> data) throws IOException
  {
    HDSFileWriter fw = null;
    BucketFileMeta fileMeta = null;
    for (Map.Entry<Slice, byte[]> dataEntry : data.entrySet()) {
      if (fw == null) {
        // next file
        fileMeta = bucketMeta.addFile(bucket.bucketKey, dataEntry.getKey());
        fw = fileStore.getWriter(bucket.bucketKey, fileMeta.name + ".tmp");
      }

      fw.append(asArray(dataEntry.getKey()), dataEntry.getValue());
      if ( fw.getBytesWritten() > this.maxFileSize) {

        // roll file
        fw.close();
        fileStore.rename(bucket.bucketKey, fileMeta.name + ".tmp", fileMeta.name);
        fw = null;
      }
    }

    if (fw != null) {
      fw.close();
      fileStore.rename(bucket.bucketKey, fileMeta.name + ".tmp", fileMeta.name);
    }
  }

  private Bucket getBucket(long bucketKey) throws IOException
  {
    Bucket bucket = this.buckets.get(bucketKey);
    if (bucket == null) {
      bucket = new Bucket();
      bucket.bucketKey = bucketKey;
      this.buckets.put(bucketKey, bucket);

      BucketMeta bmeta = getMeta(bucketKey);
      WalMeta wmeta = getWalMeta(bucketKey);
      bucket.wal = new BucketWalWriter(fileStore, bucketKey, wmeta.fileId, wmeta.offset);
      bucket.wal.setMaxWalFileSize(maxWalFileSize);

      // bmeta.componentLSN is data which is committed to disks.
      // wmeta.windowId     windowId till which data is available in WAL.
      if (bmeta.committedWid < wmeta.windowId)
      {
        bucket.recoveryInProgress = true;
        // Get last committed LSN from store, and use that for recovery.
        bucket.wal.runRecovery(this, wmeta.tailId, wmeta.tailOffset);
        bucket.recoveryInProgress = false;
      }
    }
    return bucket;
  }

  private transient Slice keyWrapper = new Slice(null, 0, 0);

  @Override
  public byte[] get(long bucketKey, byte[] key) throws IOException
  {
    keyWrapper.buffer = key;
    keyWrapper.length = key.length;

    Bucket bucket = getBucket(bucketKey);
    // check unwritten changes first
    byte[] v = bucket.writeCache.get(keyWrapper);
    if (v != null) {
      return v;
    }

    // check changes currently being flushed
    v = bucket.frozenWriteCache.get(keyWrapper);
    if (v != null) {
      return v;
    }

    BucketMeta bm = getMeta(bucketKey);
    if (bm == null) {
      throw new IllegalArgumentException("Invalid bucket key " + bucketKey);
    }

    Map.Entry<Slice, BucketFileMeta> floorEntry = bm.files.floorEntry(keyWrapper);
    if (floorEntry == null) {
      // no file for this key
      return null;
    }

    // lookup against data file
    HDSFileReader reader = bucket.readerCache.get(floorEntry.getValue().name);
    if (reader == null) {
      bucket.readerCache.put(floorEntry.getValue().name, reader = fileStore.getReader(bucketKey, floorEntry.getValue().name));
    }

    //return reader.getValue(key);
    reader.seek(key);
    Slice value = new Slice(null, 0,0);
    reader.next(new Slice(null, 0, 0), value);
    return value.buffer;
  }

  @Override
  public void put(long bucketKey, byte[] key, byte[] value) throws IOException
  {
    Bucket bucket = getBucket(bucketKey);
    /* Do not update WAL, if tuple being added is coming through recovery */
    if (!bucket.recoveryInProgress) {
      bucket.wal.append(key, value);
    }
    bucket.writeCache.put(new Slice(key, 0, key.length), value);
  }


  /**
   * Flush changes from write cache to disk.
   * New data files will be written and meta data replaced atomically.
   * The flush frequency determines availability of changes to external readers.
   * @throws IOException
   */
  private void writeDataFiles(Bucket bucket) throws IOException
  {
    // bucket keys by file
    BucketMeta bm = getMeta(bucket.bucketKey);
    TreeMap<Slice, BucketFileMeta> bucketSeqStarts = bm.files;
    Map<BucketFileMeta, Map<Slice, byte[]>> modifiedFiles = Maps.newHashMap();

    for (Map.Entry<Slice, byte[]> entry : bucket.frozenWriteCache.entrySet()) {
      // find file for key
      Map.Entry<Slice, BucketFileMeta> floorEntry = bucketSeqStarts.floorEntry(entry.getKey());
      BucketFileMeta floorFile;
      if (floorEntry != null) {
        floorFile = floorEntry.getValue();
      } else {
        floorEntry = bucketSeqStarts.firstEntry();
        if (floorEntry == null || floorEntry.getValue().name != null) {
          // no existing file or file with higher key
          floorFile = new BucketFileMeta();
        } else {
          // placeholder for new keys, move start key
          floorFile = floorEntry.getValue();
        }
        floorFile.startKey = entry.getKey();
        bucketSeqStarts.put(floorFile.startKey, floorFile);
      }

      Map<Slice, byte[]> fileUpdates = modifiedFiles.get(floorFile);
      if (fileUpdates == null) {
        modifiedFiles.put(floorFile, fileUpdates = Maps.newHashMap());
      }
      fileUpdates.put(entry.getKey(), entry.getValue());
    }

    // copy meta data on write
    BucketMeta bucketMetaCopy = kryo.copy(getMeta(bucket.bucketKey));
    HashSet<String> filesToDelete = Sets.newHashSet();

    // write modified files
    for (Map.Entry<BucketFileMeta, Map<Slice, byte[]>> fileEntry : modifiedFiles.entrySet()) {
      BucketFileMeta fileMeta = fileEntry.getKey();
      TreeMap<Slice, byte[]> fileData = Maps.newTreeMap(getKeyComparator());

      if (fileMeta.name != null) {
        // load existing file
        HDSFileReader reader = fileStore.getReader(bucket.bucketKey, fileMeta.name);
        reader.readFully(fileData);
        reader.close();
        filesToDelete.add(fileMeta.name);
      }

      // apply updates
      fileData.putAll(fileEntry.getValue());
      // new file
      writeFile(bucket, bucketMetaCopy, fileData);
    }

    // flush meta data for new files
    try {
      OutputStream os = fileStore.getOutputStream(bucket.bucketKey, FNAME_META + ".new");
      Output output = new Output(os);
      bucketMetaCopy.committedWid = windowId;
      kryo.writeClassAndObject(output, bucketMetaCopy);
      output.close();
      os.close();
      fileStore.rename(bucket.bucketKey, FNAME_META + ".new", FNAME_META);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write bucket meta data " + bucket.bucketKey, e);
    }

    // clear pending changes
    bucket.frozenWriteCache.clear();
    // switch to new version
    this.metaCache.put(bucket.bucketKey, bucketMetaCopy);

    // delete old files
    for (String fileName : filesToDelete) {
      fileStore.delete(bucket.bucketKey, fileName);
      HDSFileReader reader = bucket.readerCache.remove(fileName);
      if (reader != null) {
        reader.close();
      }
    }

    WalMeta walMeta = getWalMeta(bucket.bucketKey);
    walMeta.tailId = bucket.tailId;
    walMeta.tailOffset = bucket.tailOffset;

    bucket.wal.cleanup(walMeta.tailId);
  }

  @Override
  public void setup(OperatorContext arg0)
  {
    fileStore.init();
    writeExecutor = Executors.newSingleThreadScheduledExecutor(new NameableThreadFactory(this.getClass().getSimpleName()+"-Writer"));
  }

  @Override
  public void teardown()
  {
    for (Bucket bucket : this.buckets.values()) {
      for (HDSFileReader reader : bucket.readerCache.values()) {
        IOUtils.closeQuietly(reader);
      }
      IOUtils.closeQuietly(bucket.wal);
    }
    IOUtils.closeQuietly(fileStore);
    writeExecutor.shutdown();
  }

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
  }

  @Override
  public void endWindow()
  {
    for (final Bucket bucket : this.buckets.values()) {
      try {
        if (bucket.wal != null) {
          bucket.wal.endWindow(windowId);
          WalMeta walMeta = getWalMeta(bucket.bucketKey);
          walMeta.fileId = bucket.wal.getWalFileId();
          walMeta.offset = bucket.wal.getCommittedLength();
          walMeta.windowId = windowId;
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to flush WAL", e);
      }

      if ((bucket.writeCache.size() > this.flushSize || windowId - lastFlushWindowId > flushIntervalCount) && !bucket.writeCache.isEmpty()) {
        // ensure previous flush completed
        if (bucket.frozenWriteCache.isEmpty()) {
          bucket.frozenWriteCache = bucket.writeCache;

          bucket.committedLSN = windowId;
          bucket.tailId = bucket.wal.getWalFileId();
          bucket.tailOffset = bucket.wal.getCommittedLength();

          bucket.writeCache = Maps.newHashMap();
          LOG.debug("Flushing data to disks for bucket {} committedWid {}", bucket.bucketKey, bucket.committedLSN);
          Runnable flushRunnable = new Runnable() {
            @Override
            public void run()
            {
              try {
                writeDataFiles(bucket);
              } catch (IOException e) {
                writerError = e;
              }
            }
          };
          this.writeExecutor.execute(flushRunnable);

          if (writerError != null) {
            throw new RuntimeException("Error while flushing write cache.", this.writerError);
          }

          lastFlushWindowId = windowId;
        }
      }
    }
  }

  private WalMeta getWalMeta(long bucketKey)
  {
    WalMeta meta = walMeta.get(bucketKey);
    if (meta == null) {
      meta = new WalMeta();
      walMeta.put(bucketKey, meta);
    }
    return meta;
  }

  @Override
  public void checkpointed(long arg0)
  {
  }

  /**
   * Get meta data from cache or load it on first access
   * @param bucketKey
   * @return
   */
  private BucketMeta getMeta(long bucketKey)
  {
    BucketMeta bm = metaCache.get(bucketKey);
    if (bm == null) {
      bm = loadBucketMeta(bucketKey);
      metaCache.put(bucketKey, bm);
    }
    return bm;
  }

  private BucketMeta loadBucketMeta(long bucketKey)
  {
    BucketMeta bucketMeta = null;
    try {
      InputStream is = fileStore.getInputStream(bucketKey, FNAME_META);
      bucketMeta = (BucketMeta)kryo.readClassAndObject(new Input(is));
      is.close();
    } catch (IOException e) {
      bucketMeta = new BucketMeta(getKeyComparator());
    }
    return bucketMeta;
  }

  @Override
  public void committed(long committedWindowId)
  {
  }

  /**
   * Default key comparator that performs lexicographical comparison of the byte arrays.
   */
  public static class DefaultKeyComparator implements Comparator<Slice>
  {
    @Override
    public int compare(Slice o1, Slice o2)
    {
      return WritableComparator.compareBytes(o1.buffer, o1.offset, o1.length, o2.buffer, o2.offset, o2.length);
    }
  }

  public static class BucketFileMeta
  {
    /**
     * Name of file (relative to bucket)
     */
    public String name;
    /**
     * Lower bound sequence key
     */
    public Slice startKey;

    @Override
    public String toString()
    {
      return "BucketFileMeta [name=" + name + ", fromSeq=" + startKey + "]";
    }
  }

  /**
   * Meta data about bucket, persisted in store
   * Flushed on compaction
   */
  private static class BucketMeta
  {
    private BucketMeta(Comparator<Slice> cmp)
    {
      files = Maps.newTreeMap(cmp);
    }

    private BucketMeta()
    {
      // for serialization only
      files = null;
    }

    private BucketFileMeta addFile(long bucketKey, Slice startKey)
    {
      BucketFileMeta bfm = new BucketFileMeta();
      bfm.name = Long.toString(bucketKey) + '-' + this.fileSeq++;
      bfm.startKey = startKey;
      files.put(startKey, bfm);
      return bfm;
    }

    int fileSeq;
    long committedWid;
    final TreeMap<Slice, BucketFileMeta> files;
  }

  private static class Bucket
  {
    private long bucketKey;
    // keys that were modified and written to WAL, but not yet persisted
    private HashMap<Slice, byte[]> writeCache = Maps.newHashMap();
    // keys that are being flushed to data files
    private HashMap<Slice, byte[]> frozenWriteCache = Maps.newHashMap();
    private final HashMap<String, HDSFileReader> readerCache = Maps.newHashMap();
    private BucketWalWriter wal;
    private long committedLSN;
    private boolean recoveryInProgress;
    private long tailId;
    private long tailOffset;
  }


  @VisibleForTesting
  protected void forceWal() throws IOException
  {
    for(Bucket bucket : buckets.values())
    {
      bucket.wal.close();
    }
  }

  @VisibleForTesting
  protected int unflushedData(long bucketKey) throws IOException
  {
    Bucket b = getBucket(bucketKey);
    return b.writeCache.size();
  }

  private static final Logger LOG = LoggerFactory.getLogger(HDSBucketManager.class);

  /* Holds current file Id for WAL and current offset for WAL */
  private static class WalMeta
  {
    /* The current WAL file and offset */
    // Window Id which is written to the WAL.
    public long windowId;
    // Current Wal File sequence id
    long fileId;
    // Offset in current file after writing data for windowId.
    long offset;

    /* Flushed WAL file and offset, data till this point is flushed to disk */
    public long tailId;
    public long tailOffset;
  }
}