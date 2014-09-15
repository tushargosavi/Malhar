package com.datatorrent.demos.adsdimension;

import com.datatorrent.api.DefaultInputPort;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

class GenericEventSerializer {
  EventDescription eventDescription;

  static int putSimpleField(ByteBuffer bb, Object o, Class klass)
  {
    if (klass.equals(Integer.TYPE)) {
      bb.putInt(((Integer) o).intValue());
      return 4;
    } else if (klass.equals(Long.TYPE)) {
      bb.putLong(((Long) o).intValue());
      return 8;
    } else if (klass.equals(Float.TYPE)) {
      bb.putFloat(((Float)o).floatValue());
      return 4;
    } else if (klass.equals(Double.TYPE)) {
      bb.putDouble(((Double)o).doubleValue());
      return 8;
    } else if (klass.equals(Short.TYPE)) {
      bb.putShort(((Short)o).shortValue());
      return 2;
    } else if (klass.equals(Character.TYPE)) {
      bb.putChar(((Character)o).charValue());
      return 2;
    } else if (klass.equals(String.class)) {
      // TODO add string.
    }
    return 0;
  }

  void getKey(Map<String, String> tuple)
  {
    ByteBuffer keybb = ByteBuffer.allocate(8 + 4 * 3);
    keybb.rewind();

    for (String key : eventDescription.keys) {
      Object o = tuple.get(key);
      Class otype = eventDescription.dataDesc.get(key);
      putSimpleField(keybb, o, otype);
    }
    keybb.rewind();
  }
}

class EventDescription
{
  /* What are fields in event */
  Map<String, Class> dataDesc = Maps.newHashMap();

  /* The fields in object which forms keys */
  List<String> keys = Lists.newArrayList();

  /* fields in event which forms metrics */
  List<String> metrices = Lists.newArrayList();

  /* fields in event which forms partition keys */
  List<String> partitionKeys = Lists.newArrayList();

  /* how metrices should be aggregated */
  Map<String, String> aggrDesc = Maps.newHashMap();
}

public class MapHDSOperator extends HDSOutputOperator
{

  DefaultInputPort<Map<String, Object>> in = new DefaultInputPort<Map<String, Object>>()
  {
    @Override public void process(Map<String, Object> tuple)
    {

    }
  };

  void processTuple(Map<String, String> tuple)
  {

  }
}
