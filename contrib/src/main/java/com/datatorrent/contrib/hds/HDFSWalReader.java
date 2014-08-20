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

import com.esotericsoftware.kryo.io.Input;
import com.google.common.io.CountingInputStream;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

public class HDFSWalReader implements WALReader
{
  HDSFileAccess bfs;
  private long totalSize;
  FSDataInputStream in;
  private boolean eof = false;
  MutableKeyValue pair = null;

  public HDFSWalReader(HDSFileAccess bfs, long bucketKey, long walId) throws IOException
  {
    this.bfs = bfs;
    in = (FSDataInputStream)bfs.getInputStream(bucketKey, "WAL-" + walId);
  }

  @Override public void close() throws IOException
  {
    if (in != null) {
      in.close();
    }
  }

  @Override public void seek(long offset) throws IOException
  {
    long count = in.skipBytes((int) offset);
  }

  @Override public boolean advance() throws IOException
  {
    if (eof)
      return false;

    try {
      int keyLen = in.readInt();
      byte[] key = new byte[keyLen];
      in.read(key);

      int valLen = in.readInt();
      byte[] value = new byte[valLen];
      in.read(value);

      pair =  new MutableKeyValue(key, value);
      return true;
    } catch (EOFException ex) {
      eof = true;
      return false;
    }
  }

  @Override public MutableKeyValue get() {
    return pair;
  }

}
