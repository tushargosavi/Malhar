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
package com.datatorrent.lib.dedup;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.bucket.Bucket;
import com.datatorrent.lib.bucket.BucketManager;
import com.datatorrent.lib.bucket.Bucketable;
import com.datatorrent.lib.bucket.TimeBasedBucketManagerImpl;
import com.datatorrent.lib.counters.BasicCounters;

/**
 * This is the base implementation of a deduper, which drops duplicate events.&nbsp;
 * Subclasses must implement the convert method which turns input tuples into output tuples.
 * <p>
 * Processing of an event involves:
 * <ol>
 * <li>Finding the bucket key of an event by calling {@link BucketManager#getBucketKeyFor(Bucketable)}.</li>
 * <li>Getting the bucket from {@link BucketManager} by calling {@link BucketManager#getBucket(long)}.</li>
 * <li>
 * If the bucket is not loaded:
 * <ol>
 * <li>it requests the {@link BucketManager} to load the bucket which is a non-blocking call.</li>
 * <li>Adds the event to {@link #waitingEvents} which is a collection of events that are waiting for buckets to be loaded.</li>
 * <li>{@link BucketManager} loads the bucket and informs deduper by calling {@link #bucketLoaded(Bucket)}</li>
 * <li>The deduper then processes the waiting events in {@link #handleIdleTime()}</li>
 * </ol>
 * <li>
 * If the bucket is loaded, the operator drops the event if it is already present in the bucket; emits it otherwise.
 * </li>
 * </ol>
 * </p>
 *
 * <p>
 * Based on the assumption that duplicate events fall in the same bucket.
 * </p>
 *
 * @displayName Deduper
 * @category Deduplication
 * @tags dedupe
 *
 * @param <INPUT>  type of input tuple
 * @param <OUTPUT> type of output tuple
 * @since 0.9.4
 */
public abstract class Deduper<INPUT extends Bucketable, OUTPUT>
  implements Operator, BucketManager.Listener<INPUT>, Operator.IdleTimeHandler, Partitioner<Deduper<INPUT, OUTPUT>>
{
  /**
   * The input port on which events are received.
   */
  @InputPortFieldAnnotation(name = "input", optional = true)
  public final transient DefaultInputPort<INPUT> input = new DefaultInputPort<INPUT>()
  {
    @Override
    public final void process(INPUT tuple)
    {
      long bucketKey = bucketManager.getBucketKeyFor(tuple);
      if (bucketKey < 0) {
        return;
      } //ignore event

      Bucket<INPUT> bucket = bucketManager.getBucket(bucketKey);

      if (bucket != null && bucket.containsEvent(tuple)) {
        counters.getCounter(CounterKeys.DUPLICATE_EVENTS).increment();
        return;
      } //ignore event

      if (bucket != null && bucket.isDataOnDiskLoaded()) {
        bucketManager.newEvent(bucketKey, tuple);
        output.emit(convert(tuple));
      }
      else {
        /**
         * The bucket on disk is not loaded. So we load the bucket from the disk.
         * Before that we check if there is a pending request to load the bucket and in that case we
         * put the event in a waiting list.
         */
        boolean doLoadFromDisk = false;
        List<INPUT> waitingList = waitingEvents.get(bucketKey);
        if (waitingList == null) {
          waitingList = Lists.newArrayList();
          waitingEvents.put(bucketKey, waitingList);
          doLoadFromDisk = true;
        }
        waitingList.add(tuple);

        if (doLoadFromDisk) {
          //Trigger the storage manager to load bucketData for this bucket key. This is a non-blocking call.
          bucketManager.loadBucketData(bucketKey);
        }
      }
    }

  };
  /**
   * The output port on which deduped events are emitted.
   */
  public final transient DefaultOutputPort<OUTPUT> output = new DefaultOutputPort<OUTPUT>();
  //Check-pointed state
  @NotNull
  protected BucketManager<INPUT> bucketManager;
  //bucketKey -> list of bucketData which belong to that bucket and are waiting for the bucket to be loaded.
  @NotNull
  protected final Map<Long, List<INPUT>> waitingEvents;
  protected Set<Integer> partitionKeys;
  protected int partitionMask;
  //Non check-pointed state
  protected transient final BlockingQueue<Bucket<INPUT>> fetchedBuckets;
  private transient long sleepTimeMillis;
  private transient OperatorContext context;
  protected BasicCounters<MutableLong> counters;
  private transient long currentWindow;

  public Deduper()
  {
    waitingEvents = Maps.newHashMap();
    partitionKeys = Sets.newHashSet(0);
    partitionMask = 0;

    fetchedBuckets = new LinkedBlockingQueue<Bucket<INPUT>>();
    counters = new BasicCounters<MutableLong>(MutableLong.class);
  }

  @Override
  public void setup(OperatorContext context)
  {
    this.context = context;
    this.currentWindow = context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID);
    sleepTimeMillis = context.getValue(OperatorContext.SPIN_MILLIS);

    bucketManager.setBucketCounters(counters);
    counters.setCounter(CounterKeys.DUPLICATE_EVENTS, new MutableLong());

    bucketManager.startService(this);
    logger.debug("bucket keys at startup {}", waitingEvents.keySet());
    for (long bucketKey : waitingEvents.keySet()) {
      bucketManager.loadBucketData(bucketKey);
    }
  }

  @Override
  public void teardown()
  {
    bucketManager.shutdownService();
  }

  @Override
  public void beginWindow(long l)
  {
    currentWindow = l;
  }

  @Override
  public void endWindow()
  {
    try {
      bucketManager.blockUntilAllRequestsServiced();
      handleIdleTime();
      Preconditions.checkArgument(waitingEvents.isEmpty(), waitingEvents.keySet());
      bucketManager.endWindow(currentWindow);
    }
    catch (Throwable cause) {
      DTThrowable.rethrow(cause);
    }
    context.setCounters(counters);
  }

  @Override
  public void handleIdleTime()
  {
    if (fetchedBuckets.isEmpty()) {
      /* nothing to do here, so sleep for a while to avoid busy loop */
      try {
        Thread.sleep(sleepTimeMillis);
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    else {
      /**
       * Remove all the events from waiting list whose buckets are loaded.
       * Process these events again.
       */
      Bucket<INPUT> bucket;
      while ((bucket = fetchedBuckets.poll()) != null) {
        List<INPUT> waitingList = waitingEvents.remove(bucket.bucketKey);
        if (waitingList != null) {
          for (INPUT event : waitingList) {
            if (!bucket.containsEvent(event)) {
              bucketManager.newEvent(bucket.bucketKey, event);
              output.emit(convert(event));
            }
            else {
              counters.getCounter(CounterKeys.DUPLICATE_EVENTS).increment();
            }
          }
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void bucketLoaded(Bucket<INPUT> loadedBucket)
  {
    fetchedBuckets.add(loadedBucket);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void bucketOffLoaded(long bucketKey)
  {
  }

  public Collection<Partition<Deduper<INPUT, OUTPUT>>> rebalancePartitions(Collection<Partition<Deduper<INPUT, OUTPUT>>> partitions)
  {
    /* we do not re-balance since we do not know how to do it */
    return partitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<Deduper<INPUT, OUTPUT>>> partitions)
  {
  }

  @Override
  @SuppressWarnings({"BroadCatchBlock", "TooBroadCatch", "UseSpecificCatch"})
  public Collection<Partition<Deduper<INPUT, OUTPUT>>> definePartitions(Collection<Partition<Deduper<INPUT, OUTPUT>>> partitions, int incrementalCapacity)
  {
    if (incrementalCapacity == 0) {
      return rebalancePartitions(partitions);
    }

    //Collect the state here
    List<BucketManager<INPUT>> oldStorageManagers = Lists.newArrayList();

    Map<Long, List<INPUT>> allWaitingEvents = Maps.newHashMap();

    for (Partition<Deduper<INPUT, OUTPUT>> partition : partitions) {
      //collect all bucketStorageManagers
      oldStorageManagers.add(partition.getPartitionedInstance().bucketManager);

      //collect all waiting events
      for (Map.Entry<Long, List<INPUT>> awaitingList : partition.getPartitionedInstance().waitingEvents.entrySet()) {
        if (awaitingList.getValue().size() > 0) {
          List<INPUT> existingList = allWaitingEvents.get(awaitingList.getKey());
          if (existingList == null) {
            existingList = Lists.newArrayList();
            allWaitingEvents.put(awaitingList.getKey(), existingList);
          }
          existingList.addAll(awaitingList.getValue());
        }
      }
      partition.getPartitionedInstance().waitingEvents.clear();
    }

    final int finalCapacity = partitions.size() + incrementalCapacity;
    partitions.clear();

    Collection<Partition<Deduper<INPUT, OUTPUT>>> newPartitions = Lists.newArrayListWithCapacity(finalCapacity);
    Map<Integer, BucketManager<INPUT>> partitionKeyToStorageManagers = Maps.newHashMap();

    for (int i = 0; i < finalCapacity; i++) {
      try {
        @SuppressWarnings("unchecked")
        Deduper<INPUT, OUTPUT> deduper = this.getClass().newInstance();
        DefaultPartition<Deduper<INPUT, OUTPUT>> partition = new DefaultPartition<Deduper<INPUT, OUTPUT>>(deduper);
        newPartitions.add(partition);
      }
      catch (Throwable cause) {
        DTThrowable.rethrow(cause);
      }
    }

    DefaultPartition.assignPartitionKeys(Collections.unmodifiableCollection(newPartitions), input);
    int lPartitionMask = newPartitions.iterator().next().getPartitionKeys().get(input).mask;

    //transfer the state here
    for (Partition<Deduper<INPUT, OUTPUT>> deduperPartition : newPartitions) {
      Deduper<INPUT, OUTPUT> deduperInstance = deduperPartition.getPartitionedInstance();

      deduperInstance.partitionKeys = deduperPartition.getPartitionKeys().get(input).partitions;
      deduperInstance.partitionMask = lPartitionMask;
      logger.debug("partitions {},{}", deduperInstance.partitionKeys, deduperInstance.partitionMask);
      deduperInstance.bucketManager = bucketManager.cloneWithProperties();

      for (int partitionKey : deduperInstance.partitionKeys) {
        partitionKeyToStorageManagers.put(partitionKey, deduperInstance.bucketManager);
      }

      //distribute waiting events
      for (long bucketKey : allWaitingEvents.keySet()) {
        for (Iterator<INPUT> iterator = allWaitingEvents.get(bucketKey).iterator(); iterator.hasNext(); ) {
          INPUT event = iterator.next();
          int partitionKey = event.getEventKey().hashCode() & lPartitionMask;

          if (deduperInstance.partitionKeys.contains(partitionKey)) {
            List<INPUT> existingList = deduperInstance.waitingEvents.get(bucketKey);
            if (existingList == null) {
              existingList = Lists.newArrayList();
              deduperInstance.waitingEvents.put(bucketKey, existingList);
            }
            existingList.add(event);
            iterator.remove();
          }
        }
      }
    }
    //let storage manager and subclasses distribute state as well
    bucketManager.definePartitions(oldStorageManagers, partitionKeyToStorageManagers, lPartitionMask);
    return newPartitions;
  }

  /**
   * Sets the bucket manager.
   *
   * @param bucketManager {@link BucketManager} to be used by deduper.
   */
  public void setBucketManager(@NotNull BucketManager<INPUT> bucketManager)
  {
    this.bucketManager = Preconditions.checkNotNull(bucketManager, "storage manager");
  }

  public BucketManager<INPUT> getBucketManager()
  {
    return this.bucketManager;
  }

  /**
   * Converts the input tuple to output tuple.
   *
   * @param input input event.
   * @return output tuple derived from input.
   */
  protected abstract OUTPUT convert(INPUT input);

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Deduper)) {
      return false;
    }

    Deduper<?, ?> deduper = (Deduper<?, ?>) o;

    if (partitionMask != deduper.partitionMask) {
      return false;
    }
    if (!bucketManager.equals(deduper.bucketManager)) {
      return false;
    }
    if (partitionKeys != null ? !partitionKeys.equals(deduper.partitionKeys) : deduper.partitionKeys != null) {
      return false;
    }
    return waitingEvents.equals(deduper.waitingEvents);
  }

  @Override
  public int hashCode()
  {
    int result = bucketManager.hashCode();
    result = 31 * result + (waitingEvents.hashCode());
    result = 31 * result + (partitionKeys != null ? partitionKeys.hashCode() : 0);
    result = 31 * result + partitionMask;
    return result;
  }

  @Override
  public String toString()
  {
    return "Deduper{" + "partitionKeys=" + partitionKeys + ", partitionMask=" + partitionMask + '}';
  }

  public static enum CounterKeys
  {
    DUPLICATE_EVENTS
  }

  public static class CountersListener implements StatsListener, Serializable
  {
    @Override
    public Response processStats(BatchedOperatorStats batchedOperatorStats)
    {
      List<Stats.OperatorStats> lastWindowedStats = batchedOperatorStats.getLastWindowedStats();
      if (lastWindowedStats != null) {
        for (Stats.OperatorStats os : lastWindowedStats) {
          if (os.counters != null) {
            if (os.counters instanceof BasicCounters) {
              @SuppressWarnings("unchecked")
              BasicCounters<MutableLong> cs = (BasicCounters<MutableLong>) os.counters;
              logger.debug("operatorId:{} buckets:[in-memory:{} deleted:{} evicted:{}] events:[in-memory:{} committed-last-window:{} " +
                  "ignored:{} duplicates:{}] low:{} high:{}", batchedOperatorStats.getOperatorId(),
                cs.getCounter(BucketManager.CounterKeys.BUCKETS_IN_MEMORY),
                cs.getCounter(BucketManager.CounterKeys.DELETED_BUCKETS),
                cs.getCounter(BucketManager.CounterKeys.EVICTED_BUCKETS),
                cs.getCounter(BucketManager.CounterKeys.EVENTS_IN_MEMORY),
                cs.getCounter(BucketManager.CounterKeys.EVENTS_COMMITTED_LAST_WINDOW),
                cs.getCounter(TimeBasedBucketManagerImpl.CounterKeys.IGNORED_EVENTS), cs.getCounter(CounterKeys.DUPLICATE_EVENTS),
                cs.getCounter(TimeBasedBucketManagerImpl.CounterKeys.LOW),
                cs.getCounter(TimeBasedBucketManagerImpl.CounterKeys.HIGH));
            }
          }
        }
      }
      return null;
    }

    private static final long serialVersionUID = 201404082336L;
    protected static transient final Logger logger = LoggerFactory.getLogger(CountersListener.class);
  }

  private final static Logger logger = LoggerFactory.getLogger(Deduper.class);
}
