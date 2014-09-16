package com.datatorrent.demos.adsdimension;

import com.datatorrent.lib.bucket.Event;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EventDescription
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
  private int keyLen;
  private int valLen;
  /* Do not allow users to create object directly */
  public EventDescription() { }


  /* Generate Event description from string
     {
       "fields": [ {"publisherId": "int", "advertiserId": "int", "adUnit" : "int", "clicks":"long"],
       "keys": ["publisherId", "advertiserId", "adUnit"],
       "metrices": [ "clicks"],
       "aggrDesc" : [ "clicks":"sum"],
       "partitionKeys" : ["publisherId"]
     }
   */

  public void setDataDesc(Map<String, Class> dataDesc)
  {
    this.dataDesc = dataDesc;
  }

  public void setKeys(List<String> keys)
  {
    this.keys = keys;
  }

  public void setMetrices(List<String> metrices)
  {
    this.metrices = metrices;
  }

  public void setPartitionKeys(List<String> partitionKeys)
  {
    this.partitionKeys = partitionKeys;
  }

  public void setAggrDesc(Map<String, String> aggrDesc)
  {
    this.aggrDesc = aggrDesc;
  }

  public int getKeyLen() {
    if (keyLen == 0)
      keyLen = getSerializedLength(keys);
    return keyLen;
  }

  public int getValLen() {
    if (valLen == 0)
      valLen = getSerializedLength(metrices);
    return valLen;
  }

  public int getSerializedLength(List<String> fields) {
    int len = 0;
    for(String field : fields) {
      Class k = dataDesc.get(field);
      if (k.equals(Integer.class))
        len += 4;
      else if (k.equals(Long.class))
        len += 8;
      else if (k.equals(Float.class))
        len += 4;
      else if (k.equals(Double.class))
        len += 8;
    }
    return len;
  }

  public static EventDescription create(String desc) throws IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(org.codehaus.jackson.map.DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    EventDescription e = mapper.readValue(desc, EventDescription.class);
    return e;
  }
}
