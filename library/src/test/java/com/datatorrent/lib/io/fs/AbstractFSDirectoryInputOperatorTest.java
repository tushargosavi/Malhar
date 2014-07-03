/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.fs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.python.google.common.collect.Lists;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.io.fs.AbstractFSDirectoryInputOperator.DirectoryScanner;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.google.common.collect.Sets;

public class AbstractFSDirectoryInputOperatorTest
{
  public static class TestMeta extends TestWatcher
  {
    public String dir = null;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dir = "target/" + className + "/" + methodName;
    }
  };

  @Rule public TestMeta testMeta = new TestMeta();

  public static class TestFSDirectoryInputOperator extends AbstractFSDirectoryInputOperator<String>
  {
    @OutputPortFieldAnnotation(name = "output")
    public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
    private transient BufferedReader br = null;

    @Override
    protected InputStream openFile(Path path) throws IOException
    {
      InputStream is = super.openFile(path);
      br = new BufferedReader(new InputStreamReader(is));
      return is;
    }

    @Override
    protected void closeFile(InputStream is) throws IOException
    {
      super.closeFile(is);
      br.close();
      br = null;
      System.out.println("close File called ");
    }

    @Override
    protected String readEntity() throws IOException
    {
      return br.readLine();
    }

    @Override
    protected void emit(String tuple)
    {
      output.emit(tuple);
    }
  }

  @Test
  public void test() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
    HashSet<String> allLines = Sets.newHashSet();
    for (int file=0; file<2; file++) {
      HashSet<String> lines = Sets.newHashSet();
      for (int line=0; line<2; line++) {
        lines.add("f"+file+"l"+line);
      }
      allLines.addAll(lines);
      FileUtils.write(new File(testMeta.dir, "file"+file), StringUtils.join(lines, '\n'));
    }

    TestFSDirectoryInputOperator oper = new TestFSDirectoryInputOperator();

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
    oper.output.setSink(sink);

    oper.setDirectory(testMeta.dir);
    oper.getScanner().setFilePatternRegexp(".*file[\\d]");

    oper.setup(null);
    for (long wid=0; wid<3; wid++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
    }
    oper.teardown();

    Assert.assertEquals("number tuples", 4, queryResults.collectedTuples.size());
    Assert.assertEquals("lines", allLines, new HashSet<String>(queryResults.collectedTuples));

  }

  @Test
  public void testScannerPartitioning() throws Exception
  {
    DirectoryScanner scanner = new DirectoryScanner();
    scanner.setFilePatternRegexp(".*partition([\\d]*)");

    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    for (int file=0; file<4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00"+file), "");
    }

    FileSystem fs = FileSystem.get(FileContext.getLocalFSFileContext().getDefaultFileSystem().getUri(), new Configuration());
    List<DirectoryScanner> partitions = scanner.partition(2);
    Set<Path> allFiles = Sets.newHashSet();
    for (DirectoryScanner partition : partitions) {
      Set<Path> files = partition.scan(fs, path, Sets.<String>newHashSet());
      Assert.assertEquals("", 2, files.size());
      allFiles.addAll(files);
    }
    Assert.assertEquals("Found all files " + allFiles, 4, allFiles.size());

  }

  @Test
  public void testPartitioning() throws Exception
  {
    TestFSDirectoryInputOperator oper = new TestFSDirectoryInputOperator();
    oper.getScanner().setFilePatternRegexp(".*partition([\\d]*)");
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());

    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    for (int file=0; file<4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00"+file), "");
    }

    oper.initialPartitioning = false;
    oper.currentPartitions = 1;
    oper.partitionCount = 2;
    List<Partition<AbstractFSDirectoryInputOperator<String>>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractFSDirectoryInputOperator<String>>(oper));
    Collection<Partition<AbstractFSDirectoryInputOperator<String>>> newPartitions = oper.definePartitions(partitions, 1);
    Assert.assertEquals(2, newPartitions.size());

    int count = 0;
    for (Partition<AbstractFSDirectoryInputOperator<String>> p : newPartitions) {
      count++;
      Assert.assertNotSame(oper, p.getPartitionedInstance());
      Assert.assertNotSame(oper.getScanner(), p.getPartitionedInstance().getScanner());
      Set<String> consumed = Sets.newHashSet();
      LinkedHashSet<Path> files = p.getPartitionedInstance().getScanner().scan(FileSystem.getLocal(new Configuration(false)), path, consumed);
      System.out.println("partition " + count + " files " + files);
      Assert.assertEquals("partition " + files, 2, files.size());
    }

  }

  /**
   * Test for testing dynamic partitioning.
   * - Create 4 file with 3 records each.
   * - Create a single partition, and read all records, populating pending files in operator.
   * - Split it in two operators
   * - Try to emit records again, expected result is no record is emitted, as all files are
   *   processed.
   * - Create another 4 files with 3 records each
   * - Try to emit records again, expected result total record emitted 4 * 3 = 12.
   */
  @Test
  public void testPartitioningStateTransfer() throws Exception
  {
    TestFSDirectoryInputOperator oper = new TestFSDirectoryInputOperator();
    oper.getScanner().setFilePatternRegexp(".*partition([\\d]*)");
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());

    // Create 4 files with 3 records each.
    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    int file = 0;
    for (file=0; file<4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00"+file), "a\nb\nc\n");
    }

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
    oper.output.setSink(sink);

    int wid = 0;

    // Read all records to populate processedList in operator.
    oper.setup(null);
    for(int i = 0; i < 10; i++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
      wid++;
    }
    Assert.assertEquals("All tuples read ", 12, sink.collectedTuples.size());

    /*
     * Workaround for kryo.copy,
     * definePartitions seems to work on newly created object, with no setup and
     * emitTuples called on it. once emitTuples is called, it causes the issue while
     * processing BufferedReader (br)
     */
    TestFSDirectoryInputOperator temp = new TestFSDirectoryInputOperator();
    temp.getScanner().setFilePatternRegexp(".*partition([\\d]*)");
    temp.setDirectory(new File(testMeta.dir).getAbsolutePath());
    temp.initialPartitioning = false;
    temp.currentPartitions = 1;
    temp.partitionCount = 2;

    // Create partitions of the operator.
    List<Partition<AbstractFSDirectoryInputOperator<String>>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractFSDirectoryInputOperator<String>>(oper));
    Collection<Partition<AbstractFSDirectoryInputOperator<String>>> newPartitions = temp.definePartitions(partitions, 0);
    Assert.assertEquals(2, newPartitions.size());

    /* Collect all operators in a list */
    List<AbstractFSDirectoryInputOperator<String>> opers = Lists.newArrayList();
    for (Partition<AbstractFSDirectoryInputOperator<String>> p : newPartitions) {
      TestFSDirectoryInputOperator oi = (TestFSDirectoryInputOperator)p.getPartitionedInstance();
      oi.setup(null);
      oi.output.setSink(sink);
      opers.add(oi);
    }

    sink.clear();
    for(int i = 0; i < 10; i++) {
      for(AbstractFSDirectoryInputOperator<String> o : opers) {
        o.beginWindow(wid);
        o.emitTuples();
        o.endWindow();
      }
      wid++;
    }

    // No record should be read.
    Assert.assertEquals("No new tuples read ", 0, sink.collectedTuples.size());

    // Add four new files with 3 records each.
    for (; file<8; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00"+file), "a\nb\nc\n");
    }

    // TODO: scan is not able to find the new files without sleep.
    // check for an api to flush files to storage, so that scan
    // will be able to find them.
    Thread.sleep(5000);

    for(int i = 0; i < 10; i++) {
      for(AbstractFSDirectoryInputOperator<String> o : opers) {
        o.beginWindow(wid);
        o.emitTuples();
        o.endWindow();
      }
      wid++;
    }

    // If all files are processed only once then number of records emitted should
    // be 12.
    Assert.assertEquals("All tuples read ", 12, sink.collectedTuples.size());
  }

}
