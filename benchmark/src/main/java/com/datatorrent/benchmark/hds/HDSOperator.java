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
package com.datatorrent.benchmark.hds;

import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hds.AbstractSinglePortHDSWriter;
import com.datatorrent.contrib.hds.MutableKeyValue;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

import java.util.Arrays;

public class HDSOperator extends AbstractSinglePortHDSWriter<MutableKeyValue>
{
  public static class MutableKeyValCodec extends KryoSerializableStreamCodec<MutableKeyValue> implements HDSOperator.HDSCodec<MutableKeyValue>
  {
    @Override public byte[] getKeyBytes(MutableKeyValue mutableKeyValue)
    {
      return mutableKeyValue.getKey();
    }

    @Override public byte[] getValueBytes(MutableKeyValue mutableKeyValue)
    {
      return mutableKeyValue.getValue();
    }

    @Override public MutableKeyValue fromKeyValue(Slice key, byte[] value)
    {
      MutableKeyValue pair = new MutableKeyValue(null, null);
      pair.setKey(key.buffer);
      pair.setValue(value);
      return pair;
    }

    @Override public int getPartition(MutableKeyValue tuple)
    {
      return Arrays.hashCode(tuple.getKey());
    }
  }


  @Override protected Class<? extends HDSCodec<MutableKeyValue>> getCodecClass()
  {
    return MutableKeyValCodec.class;
  }
}
