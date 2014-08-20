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

/**
 * A instance of WAL writer interface.
 */
public interface WALWriter extends Cloneable
{
  /* Close a WAL file */
  public void close() throws IOException;

  /* Append key/value data at end */
  public void append(byte[] key, byte[] value) throws IOException;

  /* Flush log to disk */
  public void flush() throws IOException;

  /* Return number of bytes written since last flush */
  public long getUnflushedCount();

  /* Return committed size */
  public long getCommittedLen();

  /* Return size of log file */
  public long logSize();
}
