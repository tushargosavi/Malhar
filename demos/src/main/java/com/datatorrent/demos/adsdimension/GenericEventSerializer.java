package com.datatorrent.demos.adsdimension;

import com.google.common.collect.Maps;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

public class GenericEventSerializer {

  static interface FieldSerializer {
    public void putField(ByteBuffer bb, Object o);
    public Object readField(ByteBuffer bb);
    public int dataLength();
  }

  static class IntSerializer implements FieldSerializer {
    @Override public void putField(ByteBuffer bb, Object o)
    {
      if (o == null)
        bb.putInt(0);
      else
        bb.putInt(((Integer) o).intValue());
    }

    @Override public Object readField(ByteBuffer bb)
    {
      int data = bb.getInt();
      return data;
    }

    @Override public int dataLength()
    {
      return 4;
    }
  }

  static class LongSerializer implements FieldSerializer {
    @Override public void putField(ByteBuffer bb, Object o)
    {
      if (o == null)
        bb.putLong(0);
      else
        bb.putLong(((Long) o).longValue());
    }

    @Override public Object readField(ByteBuffer bb)
    {
      long data = bb.getLong();
      return data;
    }

    @Override public int dataLength()
    {
      return 8;
    }
  }

  static class FloatSerializer implements FieldSerializer {
    @Override public void putField(ByteBuffer bb, Object o)
    {
      if (o == null)
        bb.putFloat(0.0f);
      else
        bb.putFloat(((Float) o).floatValue());
    }

    @Override public Object readField(ByteBuffer bb)
    {
      float data = bb.getFloat();
      return data;
    }

    @Override public int dataLength()
    {
      return 4;
    }
  }

  static class DoubleSerializer implements FieldSerializer {
    @Override public void putField(ByteBuffer bb, Object o)
    {
      if (o == null)
        bb.putDouble(0.0d);
      else
        bb.putDouble(((Double) o).doubleValue());
    }

    @Override public Object readField(ByteBuffer bb)
    {
      double data = bb.getDouble();
      return data;
    }

    @Override public int dataLength()
    {
      return 8;
    }
  }

  EventDescription eventDescription;

  public GenericEventSerializer(EventDescription eventDescription)
  {
    this.eventDescription = eventDescription;
  }

  static Class stringToType(Class klass)
  {
    return klass;
  }

  static Map<Class, FieldSerializer> fieldSerialisers = Maps.newHashMapWithExpectedSize(4);
  static {
    fieldSerialisers.put(Integer.class, new IntSerializer());
    fieldSerialisers.put(Float.class, new FloatSerializer());
    fieldSerialisers.put(Long.class, new LongSerializer());
    fieldSerialisers.put(Double.class, new DoubleSerializer());
  }

  byte[] getKey(MapAggregateEvent event)
  {
    return getKey(event.keys);
  }

  byte[] getKey(Map<String, Object> tuple)
  {
    ByteBuffer bb = ByteBuffer.allocate(eventDescription.getKeyLen());

    bb.rewind();
    for (String key : eventDescription.keys) {
      Object o = tuple.get(key);
      fieldSerialisers.get(eventDescription.getClass(key)).putField(bb, o);
    }
    bb.rewind();
    System.out.println(Arrays.toString(bb.array()));
    return bb.array();
  }

  byte[] getValue(MapAggregateEvent event)
  {
    return getValue(event.fields);
  }

  byte[] getValue(Map<String, Object> tuple)
  {
    ByteBuffer bb = ByteBuffer.allocate(eventDescription.getValLen());
    for(String metric : eventDescription.metrices)
    {
      Object o = tuple.get(metric);
      fieldSerialisers.get(eventDescription.getClass(metric)).putField(bb, o);
    }
    System.out.println(Arrays.toString(bb.array()));
    return bb.array();
  }

  public MapAggregateEvent fromBytes(byte[] keyBytes, byte[] valBytes)
  {
    MapAggregateEvent event = new MapAggregateEvent(0);

    ByteBuffer bb = ByteBuffer.wrap(keyBytes);

    // Deserialise keys.
    for (java.lang.String key : eventDescription.keys) {
      java.lang.Object o = fieldSerialisers.get(eventDescription.getClass(key)).readField(bb);
      event.keys.put(key, o);
    }

    // Deserialize metrics
    bb = ByteBuffer.wrap(valBytes);
    for(java.lang.String metric : eventDescription.metrices)
    {
      java.lang.Object o = fieldSerialisers.get(eventDescription.getClass(metric)).readField(bb);
      event.fields.put(metric, o);
    }

    return event;
  }
}

