package com.datatorrent.demos.adsdimension.generic;

import com.datatorrent.contrib.hds.AbstractSinglePortHDSWriter;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

public class MapAggregateCodec extends KryoSerializableStreamCodec<MapAggregate> implements AbstractSinglePortHDSWriter.HDSCodec<MapAggregate>
{
  public DimensionStoreOperator operator;

  @Override
  public byte[] getKeyBytes(MapAggregate aggr)
  {
    return operator.serializer.getKey(aggr);
  }

  @Override
  public byte[] getValueBytes(MapAggregate aggr)
  {
    return operator.serializer.getValue(aggr);
  }

  @Override
  public MapAggregate fromKeyValue(byte[] key, byte[] value)
  {
    MapAggregate aggr = operator.serializer.fromBytes(key, value);
    return aggr;
  }

  @Override
  public int getPartition(MapAggregate aggr)
  {
    final int prime = 31;
    int hashCode = 1;
    for(String key : aggr.getEventSchema().keys)
    {
      if (key.equals(aggr.getEventSchema().getTimeKey()))
        continue;
      Object o = aggr.get(key);
      if (o != null)
        hashCode = hashCode * prime + aggr.get(key).hashCode();
    }
    return hashCode;
  }
}
