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
package com.datatorrent.demos.adsdimension.generic;

import com.google.common.collect.Maps;

import java.nio.ByteBuffer;
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

  EventSchema eventSchema;

  // For kryo
  protected GenericEventSerializer() {}
  public GenericEventSerializer(EventSchema eventSchema)
  {
    this.eventSchema = eventSchema;
  }

  static Map<Class<?>, FieldSerializer> fieldSerializers = Maps.newHashMapWithExpectedSize(4);
  static {
    fieldSerializers.put(Integer.class, new IntSerializer());
    fieldSerializers.put(Float.class, new FloatSerializer());
    fieldSerializers.put(Long.class, new LongSerializer());
    fieldSerializers.put(Double.class, new DoubleSerializer());
  }

  byte[] getKey(MapAggregate event)
  {
    ByteBuffer bb = ByteBuffer.allocate(eventSchema.getKeyLen());

    bb.rewind();
    for (String key : eventSchema.keys) {
      Object o = event.get(key);
      fieldSerializers.get(eventSchema.getClass(key)).putField(bb, o);
    }
    bb.rewind();
    return bb.array();
  }

  byte[] getValue(MapAggregate event)
  {
    ByteBuffer bb = ByteBuffer.allocate(eventSchema.getValLen());
    for(String metric : eventSchema.getAggregateKeys())
    {
      Object o = event.get(metric);
      fieldSerializers.get(eventSchema.getClass(metric)).putField(bb, o);
    }
    return bb.array();
  }

  public MapAggregate fromBytes(byte[] keyBytes, byte[] valBytes)
  {
    MapAggregate event = new MapAggregate(eventSchema);

    ByteBuffer bb = ByteBuffer.wrap(keyBytes);

    // Deserialize keys.
    for (java.lang.String key : eventSchema.keys) {
      java.lang.Object o = fieldSerializers.get(eventSchema.getClass(key)).readField(bb);
      event.fields.put(key, o);
    }

    // Deserialize metrics
    bb = ByteBuffer.wrap(valBytes);
    for(java.lang.String metric : eventSchema.getAggregateKeys())
    {
      java.lang.Object o = fieldSerializers.get(eventSchema.getClass(metric)).readField(bb);
      event.fields.put(metric, o);
    }

    return event;
  }
}

