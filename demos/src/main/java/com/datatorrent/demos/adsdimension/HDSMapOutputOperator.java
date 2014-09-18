package com.datatorrent.demos.adsdimension;
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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.contrib.hds.HDSBucketManager;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class HDSMapOutputOperator extends HDSBucketManager implements Partitioner<HDSMapOutputOperator>
{
  private static final Logger LOG = LoggerFactory.getLogger(HDSOutputOperator.class);

  protected boolean debug = false;

  /**
   * Partition keys identify assigned bucket(s).
   */
  protected int partitionMask;

  protected Set<Integer> partitions;
  protected EventDescription eventDesc;
  protected GenericEventSerializer serialiser;

  public GenericEventSerializer getSerialiser()
  {
    return serialiser;
  }

  public void setSerialiser(GenericEventSerializer serialiser)
  {
    this.serialiser = serialiser;
  }

  private transient StreamCodec<MapAggregateEvent> streamCodec;
  protected final SortedMap<Long, Map<MapAggregateEvent, MapAggregateEvent>> cache = Maps.newTreeMap();

  // TODO: should be aggregation interval count
  private int maxCacheSize = 5;

  private MapAggregator aggregator;

  public int getMaxCacheSize()
  {
    return maxCacheSize;
  }

  public void setMaxCacheSize(int maxCacheSize)
  {
    this.maxCacheSize = maxCacheSize;
  }

  @Override
  public void endWindow()
  {
    int expiredEntries = cache.size() - maxCacheSize;
    while(expiredEntries-- > 0){

      Map<MapAggregateEvent, MapAggregateEvent> vals = cache.remove(cache.firstKey());
      for (Entry<MapAggregateEvent, MapAggregateEvent> en : vals.entrySet()) {
        MapAggregateEvent ai = en.getValue();

        try {
          LOG.debug("Putting values in store key {} val {}", ai.keys, ai.fields);
          put(getBucketKey(ai), serialiser.getKey(ai.keys), serialiser.getValue(ai.fields));
        } catch (IOException e) {
          LOG.warn("Error putting the value", e);
        }
      }
    }

    super.endWindow();
  }

  /**
   * The input port
   */
  @InputPortFieldAnnotation(name = "in", optional = true)
  public final transient DefaultInputPort<MapAggregateEvent> input = new DefaultInputPort<MapAggregateEvent>()
  {
    @Override
    public void process(MapAggregateEvent adInfo)
    {
      Map<MapAggregateEvent, MapAggregateEvent> valMap = cache.get(adInfo.getTimestamp());
      if (valMap == null) {
        valMap = new HashMap<MapAggregateEvent, MapAggregateEvent>();
        valMap.put(adInfo, adInfo);
        cache.put(adInfo.getTimestamp(), valMap);
      } else {
        MapAggregateEvent val = valMap.get(adInfo);
        if (val == null) {
          valMap.put(adInfo, adInfo);
          return;
        } else {
          aggregator.aggregate(val, adInfo);
        }
      }
    }

    @Override
    public Class<? extends StreamCodec<MapAggregateEvent>> getStreamCodec()
    {
      return getBucketKeyStreamCodec();
    }

  };

  public long getBucketKey(MapAggregateEvent aie)
  {
    return (streamCodec.getPartition(aie) & partitionMask);
  }

  public void setEventDescriptor(EventDescription eventDesc)
  {
    this.eventDesc = eventDesc;
  }

  public EventDescription getEventDescriptor()
  {
    return eventDesc;
  }

  public static class BucketKeyStreamCodec extends KryoSerializableStreamCodec<MapAggregateEvent>
  {
    @Override
    public int getPartition(MapAggregateEvent t)
    {
      final int prime = 31;
      int hashCode = 1;
      Map<String, Object> dimensionKeys = t.keys;
      for(String key : dimensionKeys.keySet())
      {
        if (!key.equals(MapAggregateEvent.TIMESTAMP_KEY_STR))
          hashCode = hashCode * prime + t.get(key).hashCode();
      }
      return hashCode;
    }
  }

  protected Class<? extends StreamCodec<MapAggregateEvent>> getBucketKeyStreamCodec()
  {
    return BucketKeyStreamCodec.class;
  }

  @Override
  public void setup(OperatorContext arg0)
  {
    LOG.debug("Opening store {} for partitions {} {}", super.getFileStore(), new PartitionKeys(this.partitionMask, this.partitions));
    super.setup(arg0);
    try {
      this.streamCodec = getBucketKeyStreamCodec().newInstance();
      serialiser = new GenericEventSerializer(eventDesc);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create streamCodec", e);
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
  }

  @Override
  public Collection<Partition<HDSMapOutputOperator>> definePartitions(Collection<Partition<HDSMapOutputOperator>> partitions, int incrementalCapacity)
  {
    boolean isInitialPartition = partitions.iterator().next().getStats() == null;

    if (!isInitialPartition) {
      // support for dynamic partitioning requires lineage tracking
      LOG.warn("Dynamic partitioning not implemented");
      return partitions;
    }

    int totalCount = partitions.size() + incrementalCapacity;
    Kryo kryo = new Kryo();
    Collection<Partition<HDSMapOutputOperator>> newPartitions = Lists.newArrayListWithExpectedSize(totalCount);
    for (int i = 0; i < totalCount; i++) {
      // Kryo.copy fails as it attempts to clone transient fields (input port)
      // TestStoreOperator oper = kryo.copy(this);
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Output output = new Output(bos);
      kryo.writeObject(output, this);
      output.close();
      Input input = new Input(bos.toByteArray());
      HDSMapOutputOperator oper = kryo.readObject(input, this.getClass());
      newPartitions.add(new DefaultPartition<HDSMapOutputOperator>(oper));
    }

    // assign the partition keys
    DefaultPartition.assignPartitionKeys(newPartitions, input);

    for (Partition<HDSMapOutputOperator> p : newPartitions) {
      PartitionKeys pks = p.getPartitionKeys().get(input);
      p.getPartitionedInstance().partitionMask = pks.mask;
      p.getPartitionedInstance().partitions = pks.partitions;
    }

    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<HDSMapOutputOperator>> arg0)
  {
  }


  public MapAggregator getAggregator()
  {
    return aggregator;
  }


  public void setAggregator(MapAggregator aggregator)
  {
    this.aggregator = aggregator;
  }


  public boolean isDebug()
  {
    return debug;
  }


  public void setDebug(boolean debug)
  {
    this.debug = debug;
  }

}
