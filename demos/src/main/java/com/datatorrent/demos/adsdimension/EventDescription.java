package com.datatorrent.demos.adsdimension;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

public class EventDescription
{

  /* What are fields in event */
  public Map<String, Class> dataDesc = Maps.newHashMap();

  /* The fields in object which forms keys */
  public List<String> keys = Lists.newArrayList();

  /* fields in event which forms metrics */
  public List<String> metrices = Lists.newArrayList();

  /* fields in event which forms partition keys */
  public List<String> partitionKeys = Lists.newArrayList();

  /* how metrices should be aggregated */
  public Map<String, String> aggrDesc = Maps.newHashMap();
  transient private int keyLen;
  transient private int valLen;

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

  public Class getClass(String field) {
    return dataDesc.get(field);
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
      len += GenericEventSerializer.fieldSerialisers.get(k).dataLength();
    }
    return len;
  }

  public Class getType(String param)
  {
    return dataDesc.get(param);
  }
}
