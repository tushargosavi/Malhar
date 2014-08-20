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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;

import java.io.Closeable;
import java.io.IOException;

/**
 * A simple iterator type interface to read WAL
 * entries.
 */
public interface WALReader extends Closeable
{

  /* Close WAL after read */
  public void close() throws IOException;

  /* Seek to a location in WAL */
  public void seek(long offset) throws IOException;

  /* Advance WAL by one entry, returns true if it can advance, else false */
  public boolean advance() throws IOException;

  /* Return current entry from WAL */
  public MutableKeyValue get() throws IOException;
}
