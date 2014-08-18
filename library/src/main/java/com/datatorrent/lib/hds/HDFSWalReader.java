package com.datatorrent.lib.hds;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;

import java.io.DataInputStream;
import java.io.IOException;

public class HDFSWalReader<ENTRY> implements WALReader<ENTRY>
{
  HDSFileAccess bfs;

  private long totalSize;
  private long offset;
  DataInputStream in;
  HDS.WalSerializer<ENTRY> serde;

  private transient final Kryo kryo = new Kryo();
  private transient Input kin;

  public HDFSWalReader(HDSFileAccess bfs, long bucketKey, long walId, HDS.WalSerializer<ENTRY> serde) throws IOException
  {
    this.bfs = bfs;
    in = bfs.getInputStream(bucketKey, String.valueOf(walId));
    kin = new Input(in);
    totalSize = in.available();
    offset = 0;
    this.serde = serde;
  }

  @Override public void close() throws IOException
  {
    if (in != null)
      in.close();
  }

  @Override public void seek(long offset) throws IOException
  {
    if (in instanceof Seekable)
      ((FSDataInputStream)in).seek(offset);
  }

  @Override public boolean hasNext()
  {
    return (offset < totalSize);
  }

  @Override public ENTRY readNext() throws IOException
  {
    byte[] bytes = kryo.readObject(kin, byte[].class);
    ENTRY e = serde.fromBytes(bytes);
    offset = kin.position();
    return e;
  }
}
