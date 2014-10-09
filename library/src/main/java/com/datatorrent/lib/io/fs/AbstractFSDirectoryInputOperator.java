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

import com.datatorrent.api.Context.CountersAggregator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;
import com.datatorrent.lib.counters.BasicCounters;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the base implementation of a directory input operator, which scans a directory for files.&nbsp;
 * Files are then read and split into tuples, which are emitted.&nbsp;
 * Subclasses should implement the methods required to read and emit tuples from files.
 * <p>
 * Derived class defines how to read entries from the input stream and emit to the port.
 * </p>
 * <p>
 * The directory scanning logic is pluggable to support custom directory layouts and naming schemes. The default
 * implementation scans a single directory.
 * </p>
 * <p>
 * Fault tolerant by tracking previously read files and current offset as part of checkpoint state. In case of failure
 * the operator will skip files that were already processed and fast forward to the offset of the current file.
 * </p>
 * <p>
 * Supports partitioning and dynamic changes to number of partitions through property {@link #partitionCount}. The
 * directory scanner is responsible to only accept the files that belong to a partition.
 * </p>
 * <p>
 * This class supports retrying of failed files by putting them into failed list, and retrying them after pending
 * files are processed. Retrying is disabled when maxRetryCount is set to zero.
 * </p>
 * @displayName FS Directory Scan Input
 * @category Input
 * @tags fs, file, input operator
 *
 * @param <T> The type of the object that this input operator reads.
 * @since 1.0.2
 */
public abstract class AbstractFSDirectoryInputOperator<T> implements InputOperator, Partitioner<AbstractFSDirectoryInputOperator<T>>, StatsListener
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFSDirectoryInputOperator.class);

  @NotNull
  protected String directory;
  @NotNull
  protected DirectoryScanner scanner = new DirectoryScanner();
  protected int scanIntervalMillis = 5000;
  protected int offset;
  protected String currentFile;
  protected Set<String> processedFiles = new HashSet<String>();
  protected int emitBatchSize = 1000;
  protected int currentPartitions = 1 ;
  protected int partitionCount = 1;
  private int retryCount = 0;
  private int maxRetryCount = 5;
  transient protected int skipCount = 0;
  private transient OperatorContext context;

  private BasicCounters<MutableLong> fileCounters = new BasicCounters<MutableLong>(MutableLong.class);
  protected MutableLong globalNumberOfFailures = new MutableLong();
  protected MutableLong localNumberOfFailures = new MutableLong();
  protected MutableLong globalNumberOfRetries = new MutableLong();
  protected MutableLong localNumberOfRetries = new MutableLong();
  private transient MutableLong globalProcessedFileCount = new MutableLong();
  private transient MutableLong localProcessedFileCount = new MutableLong();
  private transient MutableLong pendingFileCount = new MutableLong();

  /**
   * Class representing failed file, When read fails on a file in middle, then the file is
   * added to failedList along with last read offset.
   * The files from failedList will be processed after all pendingFiles are processed, but
   * before checking for new files.
   * failed file is retried for maxRetryCount number of times, after that the file is
   * ignored.
   */
  protected static class FailedFile {
    String path;
    int   offset;
    int    retryCount;
    long   lastFailedTime;

    /* For kryo serialization */
    protected FailedFile() {}

    protected FailedFile(String path, int offset) {
      this.path = path;
      this.offset = offset;
      this.retryCount = 0;
    }

    protected FailedFile(String path, int offset, int retryCount) {
      this.path = path;
      this.offset = offset;
      this.retryCount = retryCount;
    }

    @Override
    public String toString()
    {
      return "FailedFile[" +
          "path='" + path + '\'' +
          ", offset=" + offset +
          ", retryCount=" + retryCount +
          ", lastFailedTime=" + lastFailedTime +
          ']';
    }
  }

  /**
   * Enums for aggregated counters about file processing.
   * <p/>
   * Contains the enums representing number of files processed, number of
   * pending files, number of file errors, and number of retries.
   * <p/>
   * @since 1.0.4
   */
  public static enum AggregatedFileCounters
  {
    /**
     * The number of files processed by the logical operator up until this.
     * point in time
     */
    PROCESSED_FILES,
    /**
     * The number of files waiting to be processed by the logical operator.
     */
    PENDING_FILES,
    /**
     * The number of IO errors encountered by the logical operator.
     */
    NUMBER_OF_ERRORS,
    /**
     * The number of times the logical operator tried to resume reading a file
     * on which it encountered an error.
     */
    NUMBER_OF_RETRIES
  }

  /**
   * The enums used to track statistics about the
   * AbstractFSDirectoryInputOperator.
   */
  protected static enum FileCounters
  {
    /**
     * The number of files that were in the processed list up to the last
     * repartition of the operator.
     */
    GLOBAL_PROCESSED_FILES,
    /**
     * The number of files added to the processed list by the physical operator
     * since the last repartition.
     */
    LOCAL_PROCESSED_FILES,
    /**
     * The number of io errors encountered up to the last repartition of the
     * operator.
     */
    GLOBAL_NUMBER_OF_FAILURES,
    /**
     * The number of failures encountered by the physical operator since the
     * last repartition.
     */
    LOCAL_NUMBER_OF_FAILURES,
    /**
     * The number of retries encountered by the physical operator up to the last
     * repartition.
     */
    GLOBAL_NUMBER_OF_RETRIES,
    /**
     * The number of retries encountered by the physical operator since the last
     * repartition.
     */
    LOCAL_NUMBER_OF_RETRIES,
    /**
     * The number of files pending on the physical operator.
     */
    PENDING_FILES
  }

  /**
   * A counter aggregator for AbstractFSDirectoryInputOperator.
   * <p/>
   * In order for this CountersAggregator to be used on your operator, you must
   * set it within your application like this.
   * <p/>
   * <code>
   * dag.getOperatorMeta("fsinputoperator").getAttributes().put(OperatorContext.COUNTERS_AGGREGATOR,
   *                                                            new AbstractFSDirectoryInputOperator.FileCountersAggregator());
   * </code>
   * <p/>
   * The value of the aggregated counter can be retrieved by issuing a get
   * request to the host running your gateway like this.
   * <p/>
   * <code>
   * http://&lt;your host&gt;:9090/ws/v1/applications/&lt;your app id&gt;/logicalPlan/operators/&lt;operatorname&gt;/aggregation
   * </code>
   * <p/>
   * @since 1.0.4
   */
  public final static class FileCountersAggregator implements CountersAggregator,
                                                        Serializable
  {
    private static final long serialVersionUID = 201409041428L;
    MutableLong totalLocalProcessedFiles = new MutableLong();
    MutableLong pendingFiles = new MutableLong();
    MutableLong totalLocalNumberOfFailures = new MutableLong();
    MutableLong totalLocalNumberOfRetries = new MutableLong();

    @Override
    @SuppressWarnings("unchecked")
    public Object aggregate(Collection<?> countersList)
    {
      if(countersList.isEmpty()) {
        return null;
      }

      BasicCounters<MutableLong> tempFileCounters = (BasicCounters<MutableLong>) countersList.iterator().next();
      MutableLong globalProcessedFiles = tempFileCounters.getCounter(FileCounters.GLOBAL_PROCESSED_FILES);
      MutableLong globalNumberOfFailures = tempFileCounters.getCounter(FileCounters.GLOBAL_NUMBER_OF_FAILURES);
      MutableLong globalNumberOfRetries = tempFileCounters.getCounter(FileCounters.GLOBAL_NUMBER_OF_RETRIES);
      totalLocalProcessedFiles.setValue(0);
      pendingFiles.setValue(0);
      totalLocalNumberOfFailures.setValue(0);
      totalLocalNumberOfRetries.setValue(0);

      for(Object fileCounters: countersList) {
        BasicCounters<MutableLong> basicFileCounters = (BasicCounters<MutableLong>) fileCounters;
        totalLocalProcessedFiles.add(basicFileCounters.getCounter(FileCounters.LOCAL_PROCESSED_FILES));
        pendingFiles.add(basicFileCounters.getCounter(FileCounters.PENDING_FILES));
        totalLocalNumberOfFailures.add(basicFileCounters.getCounter(FileCounters.LOCAL_NUMBER_OF_FAILURES));
        totalLocalNumberOfRetries.add(basicFileCounters.getCounter(FileCounters.LOCAL_NUMBER_OF_RETRIES));
      }

      globalProcessedFiles.add(totalLocalProcessedFiles);
      globalProcessedFiles.subtract(pendingFiles);
      globalNumberOfFailures.add(totalLocalNumberOfFailures);
      globalNumberOfRetries.add(totalLocalNumberOfRetries);

      BasicCounters<MutableLong> aggregatedCounters = new BasicCounters<MutableLong>(MutableLong.class);
      aggregatedCounters.setCounter(AggregatedFileCounters.PROCESSED_FILES, globalProcessedFiles);
      aggregatedCounters.setCounter(AggregatedFileCounters.PENDING_FILES, pendingFiles);
      aggregatedCounters.setCounter(AggregatedFileCounters.NUMBER_OF_ERRORS, totalLocalNumberOfFailures);
      aggregatedCounters.setCounter(AggregatedFileCounters.NUMBER_OF_RETRIES, totalLocalNumberOfRetries);

      return aggregatedCounters;
    }
  }

  protected long lastRepartition = 0;
  private transient boolean emit = true;
  protected boolean idempotentEmit = false;
  /* List of unfinished files */
  protected Queue<FailedFile> unfinishedFiles = new LinkedList<FailedFile>();
  /* List of failed file */
  protected Queue<FailedFile> failedFiles = new LinkedList<FailedFile>();

  protected transient FileSystem fs;
  protected transient Configuration configuration;
  protected transient long lastScanMillis;
  protected transient Path filePath;
  protected transient InputStream inputStream;
  protected Set<String> pendingFiles = new LinkedHashSet<String>();

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

  /**
   * Returns the frequency with which new files are scanned for in milliseconds.
   * @return The scan interval in milliseconds.
   */
  public int getScanIntervalMillis()
  {
    return scanIntervalMillis;
  }

  /**
   * Sets the frequency with which new files are scanned for in milliseconds.
   * @param scanIntervalMillis The scan interval in milliseconds.
   */
  public void setScanIntervalMillis(int scanIntervalMillis)
  {
    this.scanIntervalMillis = scanIntervalMillis;
  }

  /**
   * Returns the number of tuples emitted in a batch. If the operator is
   * idempotent then this is the number of tuples emitted in a window.
   * @return The number of tuples emitted in a batch.
   */
  public int getEmitBatchSize()
  {
    return emitBatchSize;
  }

  /**
   * Sets the number of tuples to emit in a batch. If the operator is
   * idempotent then this is the number of tuples emitted in a window.
   * @param emitBatchSize The number of tuples to emit in a batch.
   */
  public void setEmitBatchSize(int emitBatchSize)
  {
    this.emitBatchSize = emitBatchSize;
  }

  /**
   * Sets whether the operator is idempotent or not.
   * @param idempotentEmit If this is true, then the operator
   */
  public void setIdempotentEmit(boolean idempotentEmit)
  {
    this.idempotentEmit = idempotentEmit;
  }

  /**
   *
   * @return
   */
  public boolean isIdempotentEmit()
  {
    return idempotentEmit;
  }

  /**
   * Returns the desired number of partitions.
   * @return the desired number of partitions.
   */
  public int getPartitionCount()
  {
    return partitionCount;
  }

  /**
   * Sets the desired number of partitions.
   * @param requiredPartitions The desired number of partitions.
   */
  public void setPartitionCount(int requiredPartitions)
  {
    this.partitionCount = requiredPartitions;
  }

  /**
   * Returns the current number of partitions for the operator.
   * @return The current number of partitions for the operator.
   */
  public int getCurrentPartitions()
  {
    return currentPartitions;
  }

  @Override
  public void setup(OperatorContext context)
  {
    globalProcessedFileCount.setValue((long) processedFiles.size());
    LOG.debug("Setup processed file count: {}", globalProcessedFileCount);
    this.context = context;

    try {
      filePath = new Path(directory);
      configuration = new Configuration();
      fs = FileSystem.newInstance(filePath.toUri(), configuration);
      if(!unfinishedFiles.isEmpty()) {
        retryFailedFile(unfinishedFiles.poll());
        skipCount = 0;
      } else if(!failedFiles.isEmpty()) {
        retryFailedFile(failedFiles.poll());
        skipCount = 0;
      }
      long startTime = System.currentTimeMillis();
      LOG.info("Continue reading {} from index {} time={}", currentFile, offset, startTime);
      // fast forward to previous offset
      if(inputStream != null) {
        for(int index = 0; index < offset; index++) {
          readEntity();
        }
      }
      LOG.info("Read offset={} records in setup time={}", offset, System.currentTimeMillis() - startTime);
    }
    catch (IOException ex) {
      failureHandling(ex);
    }

    fileCounters.setCounter(FileCounters.GLOBAL_PROCESSED_FILES,
                            globalProcessedFileCount);
    fileCounters.setCounter(FileCounters.LOCAL_PROCESSED_FILES,
                            localProcessedFileCount);
    fileCounters.setCounter(FileCounters.GLOBAL_NUMBER_OF_FAILURES,
                            globalNumberOfFailures);
    fileCounters.setCounter(FileCounters.LOCAL_NUMBER_OF_FAILURES,
                            localNumberOfFailures);
    fileCounters.setCounter(FileCounters.GLOBAL_NUMBER_OF_RETRIES,
                            globalNumberOfRetries);
    fileCounters.setCounter(FileCounters.LOCAL_NUMBER_OF_RETRIES,
                            localNumberOfRetries);
    fileCounters.setCounter(FileCounters.PENDING_FILES,
                            pendingFileCount);

  }

  @Override
  public void teardown()
  {
    IOException savedException = null;
    boolean fileFailed = false;

    try {
      if(inputStream != null) {
        inputStream.close();
      }
    }
    catch (IOException ex) {
      savedException = ex;
      fileFailed = true;
    }

    boolean fsFailed = false;

    try {
      fs.close();
    }
    catch (IOException ex) {
      savedException = ex;
      fsFailed = true;
    }

    if(savedException != null) {
      String errorMessage = "";

      if(fileFailed) {
        errorMessage += "Failed to close " + currentFile + ". ";
      }

      if(fsFailed) {
        errorMessage += "Failed to close filesystem.";
      }

      throw new RuntimeException(errorMessage, savedException);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    emit = true;
  }

  @Override
  public void endWindow()
  {
    if(context != null) {
      pendingFileCount.setValue(pendingFiles.size() +
                                     failedFiles.size() +
                                     unfinishedFiles.size());

      if(currentFile != null) {
        pendingFileCount.increment();
      }

      context.setCounters(fileCounters);
    }
  }

  @Override
  public void emitTuples()
  {
    //emit will be true if the operator is not idempotent. If the operator is
    //idempotent then emit will be true the first time emitTuples is called
    //within a window and false the other times emit tuples is called within a
    //window
    if(emit)
    {
      if (inputStream == null) {
        try {
          if(!unfinishedFiles.isEmpty()) {
            retryFailedFile(unfinishedFiles.poll());
          }
          else if (!pendingFiles.isEmpty()) {
            String newPathString = pendingFiles.iterator().next();
            pendingFiles.remove(newPathString);
            this.inputStream = openFile(new Path(newPathString));
          }
          else if (!failedFiles.isEmpty()) {
            retryFailedFile(failedFiles.poll());
          }
          else {
            scanDirectory();
          }
        } catch (IOException ex) {
          failureHandling(ex);
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

            // If skipCount is non zero, then failed file recovery is going on, skipCount is
            // used to prevent already emitted records from being emitted again during recovery.
            // When failed file is open, skipCount is set to the last read offset for that file.
            //
            if (skipCount == 0) {
              offset++;
              emit(line);
            }
            else
              skipCount--;
          }
        } catch (IOException e) {
          failureHandling(e);
        }
      }
      //If the operator is idempotent, do nothing on other calls to emittuples
      //within the same window
      if(idempotentEmit)
      {
        emit = false;
      }
    }
  }

  /**
   * Scans the directory for new files.
   */
  protected void scanDirectory()
  {
    if(System.currentTimeMillis() - scanIntervalMillis >= lastScanMillis) {
      Set<Path> newPaths = scanner.scan(fs, filePath, processedFiles);

      for(Path newPath : newPaths) {
        String newPathString = newPath.toString();
        pendingFiles.add(newPathString);
        processedFiles.add(newPathString);
        localProcessedFileCount.increment();
      }

      lastScanMillis = System.currentTimeMillis();
    }
  }

  /**
   * Helper method for handling IOExceptions.
   * @param e The caught IOException.
   */
  private void failureHandling(Exception e)
  {
    localNumberOfFailures.increment();
    if(maxRetryCount <= 0) {
      throw new RuntimeException(e);
    }
    LOG.error("FS reader error", e);
    addToFailedList();
  }

  protected void addToFailedList() {

    FailedFile ff = new FailedFile(currentFile, offset, retryCount);

    try {
      // try to close file
      if (this.inputStream != null)
        this.inputStream.close();
    } catch(IOException e) {
      localNumberOfFailures.increment();
      LOG.error("Could not close input stream on: " + currentFile);
    }

    ff.retryCount ++;
    ff.lastFailedTime = System.currentTimeMillis();
    ff.offset = this.offset;

    // Clear current file state.
    this.currentFile = null;
    this.inputStream = null;
    this.offset = 0;

    if (ff.retryCount > maxRetryCount)
      return;

    localNumberOfRetries.increment();
    LOG.info("adding to failed list path {} offset {} retry {}", ff.path, ff.offset, ff.retryCount);
    failedFiles.add(ff);
  }

  protected InputStream retryFailedFile(FailedFile ff)  throws IOException
  {
    LOG.info("retrying failed file {} offset {} retry {}", ff.path, ff.offset, ff.retryCount);
    String path = ff.path;
    this.inputStream = openFile(new Path(path));
    this.offset = ff.offset;
    this.retryCount = ff.retryCount;
    this.skipCount = ff.offset;
    return this.inputStream;
  }

  protected InputStream openFile(Path path) throws IOException
  {
    LOG.info("opening file {}", path);
    InputStream input = fs.open(path);
    currentFile = path.toString();
    offset = 0;
    retryCount = 0;
    skipCount = 0;
    return input;
  }

  protected void closeFile(InputStream is) throws IOException
  {
    LOG.info("closing file {} offset {}", currentFile, offset);

    if (is != null)
      is.close();

    currentFile = null;
    inputStream = null;
  }

  @Override
  public Collection<Partition<AbstractFSDirectoryInputOperator<T>>> definePartitions(Collection<Partition<AbstractFSDirectoryInputOperator<T>>> partitions, int incrementalCapacity)
  {
    lastRepartition = System.currentTimeMillis();

    int totalCount = computedNewPartitionCount(partitions, incrementalCapacity);

    LOG.debug("Computed new partitions: {}", totalCount);

    if (totalCount == partitions.size()) {
      return partitions;
    }

    AbstractFSDirectoryInputOperator<T> tempOperator = partitions.iterator().next().getPartitionedInstance();

    MutableLong tempGlobalNumberOfRetries = tempOperator.globalNumberOfRetries;
    MutableLong tempGlobalNumberOfFailures = tempOperator.globalNumberOfRetries;

    /*
     * Build collective state from all instances of the operator.
     */
    Set<String> totalProcessedFiles = Sets.newHashSet();
    Set<FailedFile> currentFiles = Sets.newHashSet();
    List<DirectoryScanner> oldscanners = Lists.newLinkedList();
    List<FailedFile> totalFailedFiles = Lists.newLinkedList();
    List<String> totalPendingFiles = Lists.newLinkedList();
    for(Partition<AbstractFSDirectoryInputOperator<T>> partition : partitions) {
      AbstractFSDirectoryInputOperator<T> oper = partition.getPartitionedInstance();
      totalProcessedFiles.addAll(oper.processedFiles);
      totalFailedFiles.addAll(oper.failedFiles);
      totalPendingFiles.addAll(oper.pendingFiles);
      currentFiles.addAll(unfinishedFiles);
      tempGlobalNumberOfRetries.add(oper.localNumberOfRetries);
      tempGlobalNumberOfFailures.add(oper.localNumberOfFailures);
      if (oper.currentFile != null)
        currentFiles.add(new FailedFile(oper.currentFile, oper.offset));
      oldscanners.add(oper.getScanner());
    }

    /*
     * Create partitions of scanners, scanner's partition method will do state
     * transfer for DirectoryScanner objects.
     */
    List<DirectoryScanner> scanners = scanner.partition(totalCount, oldscanners);

    Kryo kryo = new Kryo();
    Collection<Partition<AbstractFSDirectoryInputOperator<T>>> newPartitions = Lists.newArrayListWithExpectedSize(totalCount);
    for (int i=0; i<scanners.size(); i++) {
      AbstractFSDirectoryInputOperator<T> oper = kryo.copy(this);
      DirectoryScanner scn = scanners.get(i);
      oper.setScanner(scn);

      // Do state transfer for processed files.
      oper.processedFiles.addAll(totalProcessedFiles);
      oper.globalNumberOfFailures = tempGlobalNumberOfRetries;
      oper.localNumberOfFailures.setValue(0);
      oper.globalNumberOfRetries = tempGlobalNumberOfFailures;
      oper.localNumberOfRetries.setValue(0);

      /* redistribute unfinished files properly */
      oper.unfinishedFiles.clear();
      oper.currentFile = null;
      oper.offset = 0;
      Iterator<FailedFile> unfinishedIter = currentFiles.iterator();
      while(unfinishedIter.hasNext()) {
        FailedFile unfinishedFile = unfinishedIter.next();
        if (scn.acceptFile(unfinishedFile.path)) {
          oper.unfinishedFiles.add(unfinishedFile);
          unfinishedIter.remove();
        }
      }

      /* transfer failed files */
      oper.failedFiles.clear();
      Iterator<FailedFile> iter = totalFailedFiles.iterator();
      while (iter.hasNext()) {
        FailedFile ff = iter.next();
        if (scn.acceptFile(ff.path)) {
          oper.failedFiles.add(ff);
          iter.remove();
        }
      }

      /* redistribute pending files properly */
      oper.pendingFiles.clear();
      Iterator<String> pendingFilesIterator = totalPendingFiles.iterator();
      while(pendingFilesIterator.hasNext()) {
        String pathString = pendingFilesIterator.next();
        if(scn.acceptFile(pathString)) {
          oper.pendingFiles.add(pathString);
          pendingFilesIterator.remove();
        }
      }
      newPartitions.add(new DefaultPartition<AbstractFSDirectoryInputOperator<T>>(oper));
    }

    LOG.info("definePartitions called returning {} partitions", newPartitions.size());
    return newPartitions;
  }

  protected int computedNewPartitionCount(Collection<Partition<AbstractFSDirectoryInputOperator<T>>> partitions, int incrementalCapacity)
  {
    boolean isInitialParitition = partitions.iterator().next().getStats() == null;

    if (isInitialParitition && partitionCount == 1) {
      partitionCount = currentPartitions = partitions.size() + incrementalCapacity;
    } else {
      incrementalCapacity = partitionCount - currentPartitions;
    }

    int totalCount = partitions.size() + incrementalCapacity;
    LOG.info("definePartitions trying to create {} partitions, current {}  required {}", totalCount, partitionCount, currentPartitions);
    return totalCount;
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractFSDirectoryInputOperator<T>>> partitions)
  {
    currentPartitions = partitions.size();
  }

  /**
   * Read the next item from the stream. Depending on the type of stream, this could be a byte array, line or object.
   * Upon return of null, the stream will be considered fully consumed.
   * @throws IOException
   * @return Depending on the type of stream an object is returned. When null is returned the stream is consumed.
   */
  abstract protected T readEntity() throws IOException;

  /**
   * Emit the tuple on the port
   * @param tuple
   */
  abstract protected void emit(T tuple);


  /**
   * Repartition is required when number of partitions are not equal to required
   * partitions.
   * @param batchedOperatorStats the stats to use when repartitioning.
   * @return Returns the stats listener response.
   */
  @Override
  public Response processStats(BatchedOperatorStats batchedOperatorStats)
  {
    Response res = new Response();
    res.repartitionRequired = false;
    if (currentPartitions != partitionCount) {
      LOG.info("processStats: trying repartition of input operator current {} required {}", currentPartitions, partitionCount);
      res.repartitionRequired = true;
    }
    return res;
  }

  /**
   * Returns the maximum number of times the operator will attempt to process
   * a file on which it encounters an error.
   * @return The maximum number of times the operator will attempt to process a
   * file on which it encounters an error.
   */
  public int getMaxRetryCount()
  {
    return maxRetryCount;
  }

  /**
   * Sets the maximum number of times the operator will attempt to process
   * a file on which it encounters an error.
   * @param maxRetryCount The maximum number of times the operator will attempt
   * to process a file on which it encounters an error.
   */
  public void setMaxRetryCount(int maxRetryCount)
  {
    this.maxRetryCount = maxRetryCount;
  }

  /**
   * The class that is used to scan for new files in the directory for the
   * AbstractFSDirectoryInputOperator.
   */
  public static class DirectoryScanner implements Serializable
  {
    private static final long serialVersionUID = 4535844463258899929L;
    private String filePatternRegexp;
    private transient Pattern regex = null;
    private int partitionIndex;
    private int partitionCount;
    private final transient HashSet<String> ignoredFiles = new HashSet<String>();

    public String getFilePatternRegexp()
    {
      return filePatternRegexp;
    }

    public void setFilePatternRegexp(String filePatternRegexp)
    {
      this.filePatternRegexp = filePatternRegexp;
      this.regex = null;
    }

    public Pattern getRegex() {
      if (this.regex == null && this.filePatternRegexp != null)
        this.regex = Pattern.compile(this.filePatternRegexp);
      return this.regex;
    }

    public int getPartitionCount() {
      return partitionCount;
    }

    public int getPartitionIndex() {
      return partitionIndex;
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

          if (ignoredFiles.contains(filePathStr)) {
            continue;
          }

          if (acceptFile(filePathStr)) {
            LOG.debug("Found {}", filePathStr);
            pathSet.add(path);
          } else {
            // don't look at it again
            ignoredFiles.add(filePathStr);
          }
        }
      } catch (FileNotFoundException e) {
        LOG.warn("Failed to list directory {}", filePath, e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return pathSet;
    }

    protected boolean acceptFile(String filePathStr)
    {
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
      if (filePatternRegexp != null && this.regex == null) {
        regex = Pattern.compile(this.filePatternRegexp);
      }

      if (regex != null)
      {
        Matcher matcher = regex.matcher(filePathStr);
        if (!matcher.matches()) {
          return false;
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
      return "DirectoryScanner [filePatternRegexp=" + filePatternRegexp + " partitionIndex=" +
          partitionIndex + " partitionCount=" + partitionCount + "]";
    }
  }
}
