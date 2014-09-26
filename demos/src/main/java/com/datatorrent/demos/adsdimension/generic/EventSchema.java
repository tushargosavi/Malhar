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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Describes schema for performing dimensional computation on a stream of Map<String,Object> tuples.
 *
 * Schema can be constructed from following JSON string:
 *
 * {
 *   // Keys for dimensional computation.  If dimensions relationship is undefined,
 *   // all permutation of keys are used to generate the dimensional aggregates.
 *   // Data types supported: int, long, float, double
 *   "keys": {"publisherId":"int", "advertiserId":"int", "adUnit":"int"},
 *
 *   // OPTIONAL specification for dimensional combinations
 *   // If omitted, all combinations of keys are grouped by MINUTE
 *   // Supported time groupings: MINUTE, HOUR, DAY
 *   "dimensions": ["MINUTE:publisherId:advertiserId", "HOUR:advertiserId:adUnit"],
 *
 *   // Fields to aggregate with matching operation types and data types
 *   // Possible operations include: SUM, AVG, MIN, MAX
 *   // Data types supported: int, long, float, double
 *   "aggregates": { "clicks": "SUM:long", "price": "SUM:long" },
 *
 *   // Name of the timestamp fields with time specified in milliseconds ( since Jan 1, 1970 GMT )
 *   // Data type is implied to be: long
 *   "time": "timestamp",
 * }
 *
 *
 */
public class EventSchema implements Serializable
{
  private static final long serialVersionUID = 4586481500190519858L;

  /* What are fields in event */
  public Map<String, Class<?>> dataDesc = Maps.newHashMap();

  /* The fields in object which forms keys */
  public List<String> keys = Lists.newArrayList();

  /* how metrices should be aggregated */
  public Map<String, String> aggrDesc = Maps.newHashMap();
  transient private int keyLen;
  transient private int valLen;

  /* Do not allow users to create object directly */
  public EventSchema() { }

  public List<String> dimensions = Lists.newArrayList();


  public void setDataDesc(Map<String, Class<?>> dataDesc)
  {
    this.dataDesc = dataDesc;
  }

  public void setKeys(List<String> keys)
  {
    this.keys = keys;
  }

  public Collection<String> getMetrices() {
    return aggrDesc.keySet();
  }

  public void setAggrDesc(Map<String, String> aggrDesc)
  {
    this.aggrDesc = aggrDesc;
  }

  public Class<?> getClass(String field) {
    return dataDesc.get(field);
  }

  public int getKeyLen() {
    if (keyLen == 0)
      keyLen = getSerializedLength(keys);
    return keyLen;
  }

  public int getValLen() {
    if (valLen == 0)
      valLen = getSerializedLength(getMetrices());
    return valLen;
  }

  public int getSerializedLength(Collection<String> fields) {
    int len = 0;
    for(String field : fields) {
      Class<?> k = dataDesc.get(field);
      len += GenericEventSerializer.fieldSerializers.get(k).dataLength();
    }
    return len;
  }

  public Class<?> getType(String param)
  {
    return dataDesc.get(param);
  }
}
