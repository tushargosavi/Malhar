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
package com.datatorrent.contrib.zmq;

/**
 * This is a simple single port ZeroMQ input operator.&nbsp;
 * This simple operator will automatically receive data from a pusher,
 * and convert byte messages into strings which are then emitted as tuples.
 * <p></p>
 * @displayName Simple Single Port ZeroMQ Pull String Input
 * @category Messaging
 * @tags output operator, string
 * @since 0.3.2
 */
public class SimpleSinglePortZeroMQPullStringInputOperator extends SimpleSinglePortZeroMQPullInputOperator<String>
{
  private SimpleSinglePortZeroMQPullStringInputOperator()
  {
    super("INVALID");
  }

  public SimpleSinglePortZeroMQPullStringInputOperator(String addr)
  {
    super(addr);
  }

  @Override
  protected String convertFromBytesToTuple(byte[] bytes)
  {
    return new String(bytes);
  }

}
