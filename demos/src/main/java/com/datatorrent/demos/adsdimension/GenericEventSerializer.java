package com.datatorrent.demos.adsdimension;

import com.google.common.collect.Maps;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

public class GenericEventSerializer {

  EventDescription eventDescription;

  public GenericEventSerializer(EventDescription eventDescription)
  {
    this.eventDescription = eventDescription;
  }

  static Class stringToType(Class klass)
  {
    return klass;
  }

  static int putSimpleField(ByteBuffer bb, Object o)
  {
    Class klass = o.getClass();
    if (klass.equals(Integer.class)) {
      bb.putInt(((Integer) o).intValue());
      return 4;
    } else if (klass.equals(Long.class)) {
      bb.putLong(((Long) o).longValue());
      return 8;
    } else if (klass.equals(Float.class)) {
      bb.putFloat(((Float)o).floatValue());
      return 4;
    } else if (klass.equals(Double.class)) {
      bb.putDouble(((Double)o).doubleValue());
      return 8;
    } else if (klass.equals(Short.class)) {
      bb.putShort(((Short)o).shortValue());
      return 2;
    } else if (klass.equals(Character.class)) {
      bb.putChar(((Character)o).charValue());
      return 2;
    } else if (klass.equals(String.class)) {
      // TODO add string.
    }
    return 0;
  }

  /* Write default values for a data type */
  static int putSimpleFieldDefault(ByteBuffer bb, Class klass)
  {
    if (klass.equals(Integer.class)) {
      bb.putInt(0);
      return 4;
    } else if (klass.equals(Long.class)) {
      bb.putLong(0L);
      return 8;
    } else if (klass.equals(Float.class)) {
      bb.putFloat(0.0f);
      return 4;
    } else if (klass.equals(Double.class)) {
      bb.putDouble(0.0);
      return 8;
    } else if (klass.equals(Short.class)) {
      bb.putShort((short)0);
      return 2;
    } else if (klass.equals(Character.class)) {
      bb.putChar((char)0);
      return 2;
    } else if (klass.equals(String.class)) {
      // TODO add string.
    }
    return 0;
  }

  static java.lang.Object readSimpleField(ByteBuffer bb, Class klass)
  {
    if (klass.equals(Integer.class)) {
      return new Integer(bb.getInt());
    } else if (klass.equals(Long.class)) {
      return new Long(bb.getLong());
    } else if (klass.equals(Float.class)) {
      return new Float(bb.getFloat());
    } else if (klass.equals(Double.class)) {
      return new Double(bb.getDouble());
    } else if (klass.equals(Short.class)) {
      return new Short(bb.getShort());
    } else if (klass.equals(Character.class)) {
      return new Character(bb.getChar());
    } else if (klass.equals(String.class)) {
      // TODO add string.
    }
    return null;
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
      if (o == null) {
        putSimpleFieldDefault(bb, stringToType(eventDescription.dataDesc.get(key)));
        continue;
      }
      putSimpleField(bb, o);
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
      /* If metric is not defined, then write default values */
      if (o == null) {
        putSimpleFieldDefault(bb, stringToType(eventDescription.dataDesc.get(metric)));
        continue;
      }
      putSimpleField(bb, o);
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
      java.lang.Object o = readSimpleField(bb, stringToType(eventDescription.dataDesc.get(key)));
      event.keys.put(key, o);
    }

    // Deserialize metrics
    bb = ByteBuffer.wrap(valBytes);
    for(java.lang.String metric : eventDescription.metrices)
    {
      java.lang.Object o = readSimpleField(bb, stringToType(eventDescription.dataDesc.get(metric)));
      event.fields.put(metric, o);
    }

    return event;
  }

  public Map<String, Object> fromBytes1(byte[] keyBytes, byte[] valBytes)
  {
    Map<java.lang.String, java.lang.Object> tuple = Maps.newHashMapWithExpectedSize(eventDescription.keys.size() + eventDescription.metrices.size());

    ByteBuffer bb = ByteBuffer.wrap(keyBytes);

    // Deserialise keys.
    for (java.lang.String key : eventDescription.keys) {
      java.lang.Object o = readSimpleField(bb, stringToType(eventDescription.dataDesc.get(key)));
      tuple.put(key, o);
    }

    // Deserialize metrics
    bb = ByteBuffer.wrap(valBytes);
    for(java.lang.String metric : eventDescription.metrices)
    {
      java.lang.Object o = readSimpleField(bb, stringToType(eventDescription.dataDesc.get(metric)));
      tuple.put(metric, o);
    }

    return tuple;
  }

}

