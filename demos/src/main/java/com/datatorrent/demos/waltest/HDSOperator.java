package com.datatorrent.demos.waltest;

import com.datatorrent.contrib.hds.AbstractSinglePortHDSWriter;
import com.datatorrent.contrib.hds.MutableKeyValue;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;


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

    @Override public MutableKeyValue fromKeyValue(byte[] key, byte[] value)
    {
      MutableKeyValue pair = new MutableKeyValue(null, null);
      pair.setKey(key);
      pair.setValue(value);
      return pair;
    }
  }


  @Override protected Class<? extends HDSCodec<MutableKeyValue>> getCodecClass()
  {
    return MutableKeyValCodec.class;
  }
}
