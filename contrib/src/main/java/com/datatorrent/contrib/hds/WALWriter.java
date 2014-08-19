package com.datatorrent.contrib.hds;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * A instance of WAL writer interface.
 */
public interface WALWriter<ENTRY> extends Cloneable
{
  /* Close a WAL file */
  public void close() throws IOException;

  /* Append data at end */
  public void append(ENTRY data) throws IOException;

  /* Flush log to disk */
  public void flush() throws IOException;

  /* Return number of bytes written since last flush */
  public long unflushedCount();

  /* Return commited size */
  public long getCommittedLen();

  /* Return size of log file */
  public long logSize();
}
