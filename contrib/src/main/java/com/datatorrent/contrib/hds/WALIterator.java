package com.datatorrent.contrib.hds;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;



public class WALIterator {

  interface WALListener {
    void beingWindow(long wid);
    void process(byte key, byte[] val);
    void endWindow(long wid);
  }

  private final long bucketKey;
  long fileId;
  long wid;
  HDSFileAccess bfs;
  DataInputStream in;
  MutableKeyValue pair;

  WALIterator(HDSFileAccess bfs, long bucketKey, long fileId, long wid) {
    this.bucketKey = bucketKey;
    this.fileId = fileId;
    this.wid = wid;
    this.bfs = bfs;
  }

  boolean advance() throws IOException
  {
    boolean found = false;
    while (true) {
      in = getInputFile();
      try {
        short wtype = in.readShort();
        WAL.WAL_ENTRY_TYPE type = WAL.WAL_ENTRY_TYPE.values()[wtype];
        switch (type) {
        case WAL_BEGIN_ENTRY:
          wid = in.readLong();
          break;
        case WAL_DATA:
          int keyLen = in.readInt();
          byte[] key = new byte[keyLen];
          int valLen = in.readInt();
          byte[] val = new byte[valLen];

          pair = new MutableKeyValue(key, val);
          found = true;
          break;
        case WAL_END_ENTRY:
          wid = in.readLong();
        }
      } catch (EOFException ex) {
        in = null;
      } catch (FileNotFoundException ex) {
        break;
      }

      if (found)
        break;
    }

    return found;
  }

  private DataInputStream getInputFile() throws IOException
  {
    if (in != null)
      return in;
    wid++;
    in = bfs.getInputStream(bucketKey, "WAL-" + wid);
    return in;
  }

  MutableKeyValue get() {
    return pair;
  }

  long getCurrentWindow()
  {
    return wid;
  }
}
