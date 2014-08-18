package com.datatorrent.lib.hds;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;

import java.io.Closeable;
import java.io.IOException;

/**
 * A simple iterator type interface to read WAL
 * entries.
 */
public interface WALReader<ENTRY> extends Closeable
{

  /* Close WAL after read */
  public void close() throws IOException;

  /* Seek to a location in WAL */
  public void seek(long offset) throws IOException;

  /* returns true if WAL has one or more unread items */
  public boolean hasNext();

  /* Return current entry from WAL */
  public ENTRY readNext() throws IOException;
}
