package com.datatorrent.contrib.hdht;

import com.datatorrent.common.util.Slice;

import java.io.IOException;
import java.util.TreeMap;

public class WrappedReader implements HDHTFileAccess.HDSFileReader
{
  private HDHTFileAccess.HDSFileReader reader;
  private boolean closed = false;

  private WrappedReader() { }

  public WrappedReader(HDHTFileAccess.HDSFileReader reader) {
    this.reader = reader;
  }

  @Override public boolean seek(Slice key) throws IOException
  {
    try {
      return reader.seek(key);
    } catch (NullPointerException ex) {
      if (closed)
        throw new IOException("Stream is closed");
      else
        throw ex;
    }
  }

  @Override public boolean next(Slice key, Slice value) throws IOException
  {
    return reader.next(key, value);
  }

  @Override public void close() throws IOException
  {
    try {
      closed = true;
      reader.close();
    } finally {
      // make reader null to avoid any other problem. let it cause NPE in seek and
      // handle it in seek to convert in .
      reader = null;
    }
  }

  @Override public void readFully(TreeMap<Slice, byte[]> data) throws IOException
  {
    reader.readFully(data);
  }

  @Override public void reset() throws IOException
  {
    reader.reset();
  }
}
