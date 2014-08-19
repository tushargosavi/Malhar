package com.datatorrent.contrib.hds;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.io.CountingInputStream;

import java.io.DataInputStream;
import java.io.IOException;

public class HDFSWalReader<ENTRY> implements WALReader<ENTRY>
{
  HDSFileAccess bfs;
  CountingInputStream cin;
  private long totalSize;
  private long offset;
  DataInputStream in;
  HDS.WalSerializer<ENTRY> codec;

  private transient final Kryo kryo = new Kryo();
  private transient Input kin;

  public HDFSWalReader(HDSFileAccess bfs, long bucketKey, long walId, HDS.WalSerializer<ENTRY> serde) throws IOException
  {
    this.bfs = bfs;
    in = bfs.getInputStream(bucketKey, "WAL-" + walId);
    cin = new CountingInputStream(in);
    kin = new Input(cin);
    totalSize = cin.available();
    offset = 0;
    this.codec = serde;
  }

  @Override public void close() throws IOException
  {
    if (in != null) {
      kin.close();
      cin.close();
      in.close();
    }
  }

  @Override public void seek(long offset) throws IOException
  {
    kin.skip(offset);

  }

  @Override public boolean hasNext()
  {
    if (kin == null)
      return false;
    return (kin.total() < totalSize);
  }

  @Override public ENTRY readNext() throws IOException
  {
    byte[] bytes = kryo.readObject(kin, byte[].class);
    ENTRY e = codec.fromBytes(bytes);
    offset = kin.total();
    return e;
  }
}
