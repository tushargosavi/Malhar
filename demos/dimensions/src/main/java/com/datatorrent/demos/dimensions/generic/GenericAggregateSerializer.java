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
package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.common.util.Slice;
import com.google.common.collect.Maps;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

public class GenericAggregateSerializer {


  EventSchema eventSchema;
  static Map<Class<?>, FieldSerializer> fieldSerializers = Maps.newHashMap();

  // For kryo
  protected GenericAggregateSerializer() {}
  public GenericAggregateSerializer(EventSchema eventSchema)
  {
    this.eventSchema = eventSchema;
  }

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
        bb.putInt(((Number) o).intValue());
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
        bb.putLong(0L);
      else
        bb.putLong(((Number) o).longValue());
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
        bb.putFloat(((Number) o).floatValue());
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
        bb.putDouble(((Number) o).doubleValue());
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

  static class ByteSerializer implements FieldSerializer {
    @Override public void putField(ByteBuffer bb, Object o)
    {
      byte b;
      if (o == null) {
        b = 0;
      } else {
        b = ((Byte)o).byteValue();
      }
      bb.put(b);
    }

    @Override public Object readField(ByteBuffer bb)
    {
      byte b[] = new byte[1];
      bb.get(b);
      return b[0];
    }

    @Override public int dataLength()
    {
      return 1;
    }
  }

  static class ShortSerializer implements FieldSerializer {
    @Override public void putField(ByteBuffer bb, Object o)
    {
      if (o == null)
        bb.putShort((short)0);
      else
        bb.putShort(((Short) o).shortValue());
    }

    @Override public Object readField(ByteBuffer bb)
    {
      short s = bb.getShort();
      return s;
    }

    @Override public int dataLength()
    {
      return 2;
    }
  }

  static class BooleanSerializer implements FieldSerializer {
    @Override public void putField(ByteBuffer bb, Object o)
    {
      if (o == null)
        bb.putInt(0);
      else {
        int val = ((Boolean) o).booleanValue() ? 1 : 0;
        bb.putInt(val);
      }
    }

    @Override public Object readField(ByteBuffer bb)
    {
      int val = bb.getInt();
      return (val == 1);
    }

    @Override public int dataLength()
    {
      return 4;
    }
  }

  static class CharSerializer implements FieldSerializer {
    @Override public void putField(ByteBuffer bb, Object o)
    {
      if (o == null)
        bb.putChar((char)0);
      else
        bb.putChar((Character)o);
    }

    @Override public Object readField(ByteBuffer bb)
    {
      char c = bb.getChar();
      return c;
    }

    @Override public int dataLength()
    {
      return 2;
    }
  }

  static class StringSerializer implements FieldSerializer {
    @Override public void putField(ByteBuffer bb, Object o)
    {
      byte[] bytes;
      if (o == null)
        bytes = "".getBytes();
      else
        bytes = ((String) o).getBytes();

      bb.putInt(bytes.length);
      bb.put(bytes);
    }

    @Override public Object readField(ByteBuffer bb)
    {
      int len = bb.getInt();
      byte[] bytes = new byte[len];
      bb.get(bytes);
      return new String(bytes);
    }

    @Override public int dataLength()
    {
      return 0;
    }
  }

  static {
    fieldSerializers.put(Integer.class, new IntSerializer());
    fieldSerializers.put(Float.class, new FloatSerializer());
    fieldSerializers.put(Long.class, new LongSerializer());
    fieldSerializers.put(Double.class, new DoubleSerializer());
  }

  byte[] getKey(GenericAggregate event)
  {
    ByteBuffer bb = ByteBuffer.allocate(eventSchema.getKeyLen());

    ByteArrayOutputStream bo = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(bo);

    bo.toByteArray();

    // Write timestamp as first field
    fieldSerializers.get(eventSchema.getClass(eventSchema.getTimestamp())).putField(bb, event.timestamp);
    // Write event keys
    for (int i=0; i < eventSchema.genericEventKeys.size(); i++) {
      String keyName = eventSchema.genericEventKeys.get(i);
      Object keyValue = event.keys[i];
      fieldSerializers.get(eventSchema.getClass(keyName)).putField(bb, keyValue);
    }
    return bb.array();
  }

  byte[] getValue(GenericAggregate event)
  {
    ByteBuffer bb = ByteBuffer.allocate(eventSchema.getValLen());
    for (int i=0; i < eventSchema.genericEventValues.size(); i++) {
      String aggregateName = eventSchema.genericEventValues.get(i);
      Object aggregateValue = event.aggregates[i];
      fieldSerializers.get(eventSchema.getClass(aggregateName)).putField(bb, aggregateValue);
    }
    return bb.array();
  }

  public GenericAggregate fromBytes(Slice key, byte[] valBytes)
  {
    GenericAggregate event = new GenericAggregate();
    event.keys = new Object[eventSchema.genericEventKeys.size()];
    event.aggregates = new Object[eventSchema.genericEventValues.size()];

    ByteBuffer bb = ByteBuffer.wrap(key.buffer, key.offset, key.length);

    // Deserialize timestamp
    event.timestamp = (Long)fieldSerializers.get(eventSchema.getClass(eventSchema.getTimestamp())).readField(bb);

    // Deserialize keys
    for (int i=0; i < eventSchema.genericEventKeys.size(); i++) {
      String keyName = eventSchema.genericEventKeys.get(i);
      event.keys[i] = fieldSerializers.get(eventSchema.getClass(keyName)).readField(bb);
    }

    // Deserialize aggregate data
    bb = ByteBuffer.wrap(valBytes);
    for (int i=0; i < eventSchema.genericEventValues.size(); i++) {
      String aggregateName = eventSchema.genericEventValues.get(i);
      event.aggregates[i] = fieldSerializers.get(eventSchema.getClass(aggregateName)).readField(bb);
    }
    return event;
  }
}

