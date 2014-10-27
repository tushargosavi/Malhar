/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.util.Slice;

import com.datatorrent.contrib.hds.HDSWriter;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregateEvent;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregator;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveOperator extends HDSWriter
{
  private String filepath;
  private AdInfoAggregator aggregator;
  private static final Logger LOG = LoggerFactory.getLogger(HiveOperator.class);
  private transient StreamCodec<AdInfoAggregateEvent> streamCodec;
  protected final SortedMap<Long, Map<AdInfoAggregateEvent, AdInfoAggregateEvent>> cache = Maps.newTreeMap();
  private transient final ByteBuffer valbb = ByteBuffer.allocate(8 * 4);

  private transient final ByteBuffer keybb = ByteBuffer.allocate(8 + 4 * 3);

  protected static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

  public String getFilepath()
  {
    return filepath;
  }

  public void setFilepath(String filepath)
  {
    this.filepath = filepath;
  }

  public final transient DefaultInputPort<AdInfoAggregateEvent> input = new DefaultInputPort<AdInfoAggregateEvent>()
  {
    @Override
    public void process(AdInfoAggregateEvent adInfo)
    {

      // To get string values as test
     /* StringBuilder keyBuilder = new StringBuilder(32);
       keyBuilder.append(formatter.print(event.timestamp));
       if (event.publisherId != 0) {
       keyBuilder.append("|0:").append(event.publisherId);
       }
       if (event.advertiserId != 0) {
       keyBuilder.append("|1:").append(event.advertiserId);
       }
       if (event.adUnit != 0) {
       keyBuilder.append("|2:").append(event.adUnit);
       }

       String key = keyBuilder.toString();
       LOG.info("key is" + key);*/
      Map<AdInfoAggregateEvent, AdInfoAggregateEvent> valMap = cache.get(adInfo.getTimestamp());
      if (valMap == null) {
        valMap = new HashMap<AdInfoAggregateEvent, AdInfoAggregateEvent>();
        valMap.put(adInfo, adInfo);
        cache.put(adInfo.getTimestamp(), valMap);
      }
      else {
        AdInfoAggregateEvent val = valMap.get(adInfo);
        if (val == null) {
          valMap.put(adInfo, adInfo);
          return;
        }
        else {
          aggregator.aggregate(val, adInfo);
        }
      }
      byte[] keyBytes = getKey(adInfo);
      byte[] valBytes = getValue(adInfo);
      Slice key = new Slice(keyBytes);
      LOG.info("key in byte is" + Arrays.toString(keyBytes));
      LOG.info("value is in byte" + Arrays.toString(valBytes));
      AdInfo.AdInfoAggregateEvent ae1 = bytesToAggregate(key, valBytes);
    }

    private AdInfoAggregateEvent bytesToAggregate(Slice key, byte[] valBytes)
    {
      if (key == null || valBytes == null) {
        return null;
      }

      AdInfo.AdInfoAggregateEvent ae = new AdInfo.AdInfoAggregateEvent();

      java.nio.ByteBuffer bb = ByteBuffer.wrap(key.buffer, key.offset, key.length);
      ae.timestamp = bb.getLong();
      ae.publisherId = bb.getInt();
      ae.advertiserId = bb.getInt();
      ae.adUnit = bb.getInt();

      bb = ByteBuffer.wrap(valBytes);
      ae.clicks = bb.getLong();
      ae.cost = bb.getDouble();
      ae.impressions = bb.getLong();
      ae.revenue = bb.getDouble();
      return ae;
    }

  };

  @Override
  public void endWindow()
  {

     // Map<AdInfoAggregateEvent, AdInfoAggregateEvent> vals = cache.remove(cache.firstKey());
    //for (Entry<AdInfoAggregateEvent, AdInfoAggregateEvent> en : vals.entrySet()) {
    //AdInfoAggregateEvent ai = en.getValue();
        /*try {
     // put( getKey(ai), getValue(ai));
     } catch (IOException e) {
     LOG.warn("Error putting the value", e);
     }*/
    //}
    super.endWindow();
  }

  protected byte[] getKey(AdInfo event)
  {

    byte[] data = new byte[8 + 4 * 3];
    keybb.rewind();
    keybb.putLong(event.getTimestamp());
    keybb.putInt(event.getPublisherId());
    keybb.putInt(event.getAdvertiserId());
    keybb.putInt(event.getAdUnit());
    keybb.rewind();
    keybb.get(data);
    return data;
  }

  protected byte[] getValue(AdInfoAggregateEvent event)
  {

    byte[] data = new byte[8 * 4];
    valbb.rewind();
    valbb.putLong(event.clicks);
    valbb.putDouble(event.cost);
    valbb.putLong(event.impressions);
    valbb.putDouble(event.revenue);
    valbb.rewind();
    valbb.get(data);
    return data;
  }

  /*protected Class<? extends StreamCodec<AdInfoAggregateEvent>> getBucketKeyStreamCodec()
  {
    return BucketKeyStreamCodec.class;
  }*/

  @Override
  public void setup(OperatorContext arg0)
  {
    LOG.debug("Opening store {} for partitions ", super.getFileStore());
    super.setup(arg0);
    /*try {
      this.streamCodec = getBucketKeyStreamCodec().newInstance();
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to create streamCodec", e);
    }*/
  }

  public AdInfoAggregator getAggregator()
  {
    return aggregator;
  }

  public void setAggregator(AdInfoAggregator aggregator)
  {
    this.aggregator = aggregator;
  }

  public static final DateTimeFormatter formatter = DateTimeFormat.forPattern("'m|'yyyyMMddHHmm");


}
