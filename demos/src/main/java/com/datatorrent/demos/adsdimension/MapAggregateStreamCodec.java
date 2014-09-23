package com.datatorrent.demos.adsdimension;

import com.datatorrent.contrib.hds.AbstractSinglePortHDSWriter;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;

public class MapAggregateStreamCodec extends KryoSerializableStreamCodec<MapAggregate> implements AbstractSinglePortHDSWriter.HDSCodec<MapAggregate>
{
  @Override public byte[] getKeyBytes(MapAggregate MapAggregate)
  {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Output kout = new Output(out);
    kout.writeLong(MapAggregate.getTimestamp());
    kout.flush();
    kryo.writeClassAndObject(kout, MapAggregate.keys);
    kout.flush();
    System.out.println("getKeyBytes called size = " + out.size() );
    return out.toByteArray();
  }

  @Override public byte[] getValueBytes(MapAggregate MapAggregate)
  {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Output kout = new Output(out);
    kryo.writeClassAndObject(kout, MapAggregate.fields);
    kout.flush();
    System.out.println("getValueBytes called size = " + out.size() );
    return out.toByteArray();
  }

  @Override public MapAggregate fromKeyValue(byte[] key, byte[] value)
  {
    System.out.println("Size of key " + key.length + " size of value " + value.length);
    MapAggregate aggr = new MapAggregate();
    ByteArrayInputStream in = new ByteArrayInputStream(key);
    Input kin = new Input(in);
    long timeStamp = kin.readLong();
    aggr.setTimestamp(timeStamp);
    aggr.keys = (Map<String, Object>) kryo.readClassAndObject(kin);

    in = new ByteArrayInputStream(value);
    kin = new Input(in);
    aggr.fields = (Map<String, Object>) kryo.readClassAndObject(kin);

    return aggr;
  }

  @Override public int getPartition(MapAggregate MapAggregate)
  {
    return MapAggregate.keys.hashCode();
  }
}
