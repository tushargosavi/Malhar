package com.datatorrent.lib.hds;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.DataOutputStream;
import java.io.IOException;



public class HDFSWalWritter<Entry> implements WALWriter<Entry>
{
  transient HDSFileAccess bfs;
  transient DataOutputStream out;
  long commitedOffset;
  long offset;
  long unflushed;
  long bucketKey;
  int walId;

  HDS.WalSerializer serde;

  private transient final Kryo kryo = new Kryo();
  private transient Output kout;

  public HDFSWalWritter(HDSFileAccess bfs, long bucketKey, int walId, HDS.WalSerializer serde) throws IOException
  {
    this.bfs = bfs;
    this.bucketKey = bucketKey;
    this.walId = walId;
    out = bfs.getOutputStream(bucketKey, "WAL-" + walId);
    kout = new Output(out);
    this.serde = serde;
    offset = 0;
    unflushed = 0;
    commitedOffset = 0;
  }

  @Override public void close() throws IOException
  {
    if (out != null)
      out.close();
  }

  @Override public void append(Entry data) throws IOException
  {
    byte[] bytes = serde.toBytes(data);
    kryo.writeObject(kout, bytes);
    unflushed += bytes.length;
    offset = out.size();
  }

  @Override public void flush() throws IOException
  {
    /* close and open in append mode */
    out.flush();
    //out.close();
    //out = bfs.getOutputStream(bucketKey, "WAL-" + walId);
    commitedOffset = out.size();
    unflushed = 0;
    logger.info("flushing file new offset {}", commitedOffset);
  }

  @Override public long unflushedCount()
  {
    return unflushed;
  }

  @Override public long logSize()
  {
    return offset;
  }

  @Override public long getCommittedLen()
  {
    return commitedOffset;
  }

  private static transient Logger logger = LoggerFactory.getLogger(HDFSWalWritter.class);

  @Override
  public String toString() {
    return "HDFSWalWritter Bucket " + bucketKey + " file id " + walId;
  }
}
