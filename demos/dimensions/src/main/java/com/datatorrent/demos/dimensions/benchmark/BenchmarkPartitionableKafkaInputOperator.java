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
package com.datatorrent.demos.dimensions.benchmark;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.contrib.kafka.AbstractPartitionableKafkaInputOperator;
import kafka.message.Message;

/**
 * This operator emits one constant message for each kafka message received.&nbsp;
 * So we can track the throughput by messages emitted per second in the stram platform.
 * <p></p>
 * @displayName Benchmark Partitionable Kafka Input
 * @category Messaging
 * @tags input operator
 *
 * @since 0.9.3
 */
public class BenchmarkPartitionableKafkaInputOperator extends AbstractPartitionableKafkaInputOperator
{
  /**
   * The output port on which messages are emitted.
   */
  public transient DefaultOutputPort<String>  oport = new DefaultOutputPort<String>();

  @Override
  protected AbstractPartitionableKafkaInputOperator cloneOperator()
  {
    return new BenchmarkPartitionableKafkaInputOperator();
  }

  @Override
  protected void emitTuple(Message message)
  {
    oport.emit("Received");
  }

}
