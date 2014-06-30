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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.NotNull;

import com.datatorrent.common.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Partitioner;

/**
 * Input operator that reads files from a directory.<p/>
 * Derived class defines how to read entries from the input stream and emit to the port. Keeps track of previously read
 * files and current offset for recovery. The directory scanning logic is pluggable to support custom directory layouts
 * and naming schemes. The default implementation scans a single directory. Supports static partitioning based on the
 * directory scanner.
 */
public abstract class AbstractFSDirectoryInputOperator<T> implements InputOperator, Partitioner<AbstractFSDirectoryInputOperator<T>>
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFSDirectoryInputOperator.class);

  @NotNull
  private String directory;
  @NotNull
  private DirectoryScanner scanner = new DirectoryScanner();
  private int scanIntervalMillis = 5000;
  private int offset;
  private String currentFile;
  private final HashSet<String> processedFiles = new HashSet<String>();
  private int emitBatchSize = 1000;

  protected transient FileSystem fs;
  protected transient Configuration configuration;
  private transient long lastScanMillis;
  private transient Path filePath;
  private transient InputStream inputStream;
  private transient LinkedHashSet<Path> pendingFiles = new LinkedHashSet<Path>();

  public String getDirectory()
  {
    return directory;
  }

  public void setDirectory(String directory)
  {
    this.directory = directory;
  }

  public DirectoryScanner getScanner()
  {
    return scanner;
  }

  public void setScanner(DirectoryScanner scanner)
  {
    this.scanner = scanner;
  }

  public int getScanIntervalMillis()
  {
    return scanIntervalMillis;
  }

  public void setScanIntervalMillis(int scanIntervalMillis)
  {
    this.scanIntervalMillis = scanIntervalMillis;
  }

  public int getEmitBatchSize()
  {
    return emitBatchSize;
  }

  public void setEmitBatchSize(int emitBatchSize)
  {
    this.emitBatchSize = emitBatchSize;
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      filePath = new Path(directory);
      configuration = new Configuration();
      fs = FileSystem.newInstance(filePath.toUri(), configuration);
      if (currentFile != null && offset > 0) {
        LOG.info("Continue reading {} from index {}", currentFile, offset);
        int index = offset;
        this.inputStream = openFile(new Path(currentFile));
        // fast forward to previous offset
        while (offset < index) {
          readEntity();
          offset++;
        }
      }
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
    try {
      if (inputStream != null) {
        inputStream.close();
      }
      fs.close();
    } catch (Exception e) {
      // ignore
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  final public void emitTuples()
  {
    if (inputStream == null) {
      if (pendingFiles.isEmpty()) {
        if (System.currentTimeMillis() - scanIntervalMillis > lastScanMillis) {
          pendingFiles = scanner.scan(fs, filePath, processedFiles);
          lastScanMillis = System.currentTimeMillis();
        }
      }
      if (!pendingFiles.isEmpty())
      {
        Path path = pendingFiles.iterator().next();
        pendingFiles.remove(path);
        try {
          this.inputStream = openFile(path);
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }
    }

    if (inputStream != null) {
      try {
        int counterForTuple = 0;
        while (counterForTuple++ < emitBatchSize) {
          T line = readEntity();
          if (line == null) {
            LOG.info("done reading file ({} entries).", offset);
            closeFile(inputStream);
            break;
          }
          offset++;
          emit(line);
        }
      } catch (IOException e) {
        LOG.error("FS reader error", e);
        throw new RuntimeException("FS reader error", e);
      }
    }
  }

  protected InputStream openFile(Path path) throws IOException
  {
    LOG.info("opening file {}", path);
    InputStream input = fs.open(path);
    currentFile = path.toString();
    offset = 0;
    return input;
  }

  protected void closeFile(InputStream is) throws IOException
  {
    is.close();
    is = null;
    processedFiles.add(currentFile);
    currentFile = null;
    inputStream = null;
  }

  @Override
  public Collection<Partition<AbstractFSDirectoryInputOperator<T>>> definePartitions(Collection<Partition<AbstractFSDirectoryInputOperator<T>>> partitions, int incrementalCapacity)
  {
    int partitionCount = partitions.size() + incrementalCapacity;
    if (partitionCount == partitions.size()) {
      return partitions;
    }

    /*
    Build collective state from all instances of the operator.
     */
    Set<String> totalProcessedFiles = new HashSet<String>();
    List<Pair<String, Integer>> currentFiles = new ArrayList<Pair<String, Integer>>();
    List<DirectoryScanner> oldscanners = new LinkedList<DirectoryScanner>();
    for(Partition<AbstractFSDirectoryInputOperator<T>> partition : partitions) {
      AbstractFSDirectoryInputOperator<T> oper = partition.getPartitionedInstance();
      totalProcessedFiles.addAll(oper.processedFiles);
      if (oper.currentFile != null)
        currentFiles.add(new Pair(oper.currentFile, oper.offset));
      oldscanners.add(oper.getScanner());
    }

    List<DirectoryScanner> scanners = scanner.partition(partitionCount, oldscanners);
    Kryo kryo = new Kryo();
    Collection<Partition<AbstractFSDirectoryInputOperator<T>>> newPartitions = Lists.newArrayListWithExpectedSize(partitionCount);
    for (int i=0; i<scanners.size(); i++) {
      AbstractFSDirectoryInputOperator<T> oper = kryo.copy(this);
      DirectoryScanner scn = scanners.get(i);
      oper.setScanner(scn);

      // Do state transfer
      oper.processedFiles.clear();
      Iterator<String> iter = totalProcessedFiles.iterator();
      while (iter.hasNext()) {
        String path = iter.next();
        /* If current operator instance accepts the path, then add it to
         * it's processed files set, and remove it from global list, so
         * that next instance we won't consider the path.
         */
        if (scn.acceptFile(path)) {
          oper.processedFiles.add(path);
          iter.remove();
        }
      }

      /* set current scanning directory and offset */
      oper.currentFile = null;
      oper.offset = 0;
      for(Pair<String, Integer> current : currentFiles) {
        if (scn.acceptFile(current.getFirst())) {
          oper.currentFile = current.getFirst();
          oper.offset = current.getSecond();
          break;
        }
      }

      newPartitions.add(new DefaultPartition<AbstractFSDirectoryInputOperator<T>>(oper));
    }

    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractFSDirectoryInputOperator<T>>> partitions)
  {
  }

  /**
   * Read the next item from the stream. Depending on the type of stream, this could be a byte array, line or object.
   * Upon return of null, the stream will be considered fully consumed.
   */
  abstract protected T readEntity() throws IOException;

  /**
   * Emit the tuple on the port
   * @param tuple
   */
  abstract protected void emit(T tuple);


  public static class DirectoryScanner implements Serializable
  {
    private static final long serialVersionUID = 4535844463258899929L;
    private String filePatternRegexp;
    private transient Pattern regex = null;
    private int partitionIndex;
    private int partitionCount;

    public String getFilePatternRegexp()
    {
      return filePatternRegexp;
    }

    public void setFilePatternRegexp(String filePatternRegexp)
    {
      this.filePatternRegexp = filePatternRegexp;
      this.regex = null;
    }

    public LinkedHashSet<Path> scan(FileSystem fs, Path filePath, Set<String> consumedFiles)
    {
      if (filePatternRegexp != null && this.regex == null) {
         this.regex = Pattern.compile(this.filePatternRegexp);
      }

      LinkedHashSet<Path> pathSet = Sets.newLinkedHashSet();
      try {
        LOG.debug("Scanning {} with pattern {}", filePath, this.filePatternRegexp);
        FileStatus[] files = fs.listStatus(filePath);
        for (FileStatus status : files)
        {
          Path path = status.getPath();
          String filePathStr = path.toString();

          if (consumedFiles.contains(filePathStr)) {
            continue;
          }

          if (acceptFile(filePathStr)) {
            LOG.debug("Found {}", filePathStr);
            pathSet.add(path);
          } else {
            // don't look at it again
            consumedFiles.add(filePathStr);
          }
        }
      } catch (FileNotFoundException e) {
        LOG.warn("Failed to list directory {}", filePath, e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return pathSet;
    }

    protected boolean acceptFile(String filePathStr)
    {
      if (regex != null) {
        Matcher matcher = regex.matcher(filePathStr);
        if (!matcher.matches()) {
          return false;
        }
        if (partitionCount > 1) {
          int i = filePathStr.hashCode();
          int mod = i % partitionCount;
          if (mod < 0) {
            mod += partitionCount;
          }
          LOG.debug("partition {} {} {} {}", partitionIndex, filePathStr, i, mod);

          if (mod != partitionIndex) {
            return false;
          }
        }
      }
      return true;
    }

    public List<DirectoryScanner> partition(int count)
    {
      ArrayList<DirectoryScanner> partitions = Lists.newArrayListWithExpectedSize(count);
      for (int i=0; i<count; i++) {
        partitions.add(this.createPartition(i, count));
      }
      return partitions;
    }

    public List<DirectoryScanner>  partition(int count , Collection<DirectoryScanner> scanners) {
      return partition(count);
    }

    protected DirectoryScanner createPartition(int partitionIndex, int partitionCount)
    {
      DirectoryScanner that = new DirectoryScanner();
      that.filePatternRegexp = this.filePatternRegexp;
      that.regex = this.regex;
      that.partitionIndex = partitionIndex;
      that.partitionCount = partitionCount;
      return that;
    }

    @Override
    public String toString()
    {
      return "DirectoryScanner [filePatternRegexp=" + filePatternRegexp + "]";
    }

  }

}
