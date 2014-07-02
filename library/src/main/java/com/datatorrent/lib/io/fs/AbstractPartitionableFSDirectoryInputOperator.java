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

import com.datatorrent.api.StatsListener;

import java.util.Collection;
import java.util.Map;

/**
 * Adds support for dynamic partitioning for AbstractFSDirectoryInputOperator operator.
 * It exports a integer property requiredPartitions which specifies number of partitions
 * to be created.
 */
abstract  public class AbstractPartitionableFSDirectoryInputOperator<T> extends AbstractFSDirectoryInputOperator<T> implements StatsListener
{
  int numPartitions = 1 ;
  int requiredPartitions = 1;
  boolean initialPartitioning = true;

  public int getRequiredPartitions()
  {
    return requiredPartitions;
  }

  public void setRequiredPartitions(int requiredPartitions)
  {
    this.requiredPartitions = requiredPartitions;
  }

  /**
   * Adjust number of partition of the operator. If required partitions are greater than
   * current partitions then add more partitions limited by capacity available.
   * If required partitions are less than current partitions then reduce partitions else do nothing.
   */
  @Override
  public Collection<Partition<AbstractFSDirectoryInputOperator<T>>> definePartitions(Collection<Partition<AbstractFSDirectoryInputOperator<T>>> partitions, int incrementalCapacity)
  {
    if (initialPartitioning) {
      requiredPartitions = numPartitions = partitions.size();
      initialPartitioning = false;
    }

    // incrementalCapacity = Math.min(incrementalCapacity, (requiredPartitions - numPartitions));
    incrementalCapacity = requiredPartitions - numPartitions;
    return super.definePartitions(partitions, incrementalCapacity);
  }

  /**
   * Repartition is required when number of partitions are not equal to required
   * partitions.
   */
  @Override
  public Response processStats(BatchedOperatorStats batchedOperatorStats)
  {
    Response res = new Response();
    res.repartitionRequired = false;
    if (numPartitions != requiredPartitions) {
      LOG.info("processStats: trying repartition of input operator current {} required {}", numPartitions, requiredPartitions);
      res.repartitionRequired = true;
    }
    return res;
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractFSDirectoryInputOperator<T>>> partitions)
  {
    numPartitions = partitions.size();
    LOG.info("number of partitions {}", numPartitions);
    super.partitioned(partitions);
  }
}
