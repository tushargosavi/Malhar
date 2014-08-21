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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.DataOutputStream;
import java.io.IOException;


public class HDFSWalWriter implements WALWriter
{
  transient DataOutputStream out;
  long commitedOffset;
  long offset;
  long unflushed;
  long bucketKey;
  String name;

  public HDFSWalWriter(HDSFileAccess bfs, long bucketKey, String name) throws IOException
  {
    this.bucketKey = bucketKey;
    this.name = name;
    out = bfs.getOutputStream(bucketKey, name);
    unflushed = 0;
    commitedOffset = 0;
  }

  @Override public void close() throws IOException
  {
    if (out != null)
    {
      out.flush();
      out.close();
    }
  }

  @Override
  public void append(byte[] key, byte[] value) throws IOException
  {
    out.writeInt(key.length);
    out.write(key);
    out.writeInt(value.length);
    out.write(value);
  }

  @Override public void flush() throws IOException
  {
    out.flush();
    if (out instanceof FSDataOutputStream) {
      logger.info("calling hflush on file " + name);
      ((FSDataOutputStream) out).hflush();
      ((FSDataOutputStream) out).hsync();
    }
    commitedOffset = out.size();
    unflushed = 0;
    logger.info("flushing file new offset {}", commitedOffset);
  }

  @Override public long getUnflushedCount()
  {
    return unflushed;
  }

  @Override public long logSize()
  {
    return out.size();
  }

  @Override public long getCommittedLen()
  {
    return commitedOffset;
  }

  @Override
  public String toString() {
    return "HDFSWalWritter Bucket " + bucketKey + " fileId " + name ;
  }

  private static Logger logger = LoggerFactory.getLogger(HDFSWalWriter.class);
}
