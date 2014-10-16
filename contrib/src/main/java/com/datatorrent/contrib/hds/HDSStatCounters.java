package com.datatorrent.contrib.hds;

import com.datatorrent.api.Context;
import org.apache.commons.lang.mutable.MutableLong;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

@JsonSerialize(using = HDSStatCounters.Serializer.class)
public class HDSStatCounters implements Serializable
{
  /* total bytes written to the WAL */
  MutableLong totalBytes = new MutableLong(0);

  /* Duration in milliseconds waiting for flush call */
  MutableLong flushDuration = new MutableLong(0);

  /* Number of time WAL was flushed */
  MutableLong flushCounts = new MutableLong(0);

  /* Total number of time WAL was flushed in between two endWindows */
  MutableLong sizeBasedFlushed = new MutableLong(0);

  MutableLong recoveryCounts = new MutableLong(0);

  public static class HDSStatAggregator implements Context.CountersAggregator, Serializable
  {
    @Override public Object aggregate(Collection<?> countersList)
    {
      HDSStatCounters finalCounters = new HDSStatCounters();
      for(Object o : countersList)
      {
        @SuppressWarnings("unchecked")
        HDSStatCounters counter = (HDSStatCounters)o;

        finalCounters.totalBytes.add(counter.totalBytes);
        finalCounters.flushDuration.add(counter.flushDuration);
        finalCounters.flushCounts.add(counter.flushCounts);
      }
      return finalCounters;
    }
  }

  public static class Serializer extends JsonSerializer<HDSStatCounters> implements Serializable
  {

    @Override
    public void serialize(HDSStatCounters value, JsonGenerator jgen, SerializerProvider provider)
        throws IOException
    {
      jgen.writeObject(value);
    }

    private static final long serialVersionUID = 201406230131L;

  }
}

