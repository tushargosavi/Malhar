/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.file.tfile;

import java.io.IOException;

import org.apache.hadoop.io.file.tfile.Compression.Algorithm;

public class TestTFileLzoCodecsStreams extends TestTFileStreams {
  /**
   * Test LZO compression codec, using the same test cases as in the ByteArrays.
   */
  @Override
  public void setUp() throws IOException {
    skip = !(Algorithm.LZO.isSupported());
    if (skip) {
      System.out.println("Skipped");
    }
    init(Compression.Algorithm.LZO.getName(), "memcmp");
    if (!skip) 
      super.setUp();
  }
}
