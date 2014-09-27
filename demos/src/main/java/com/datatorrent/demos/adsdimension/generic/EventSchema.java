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
import com.google.common.collect.Sets;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Describes schema for performing dimensional computation on a stream of Map<String,Object> tuples.
 *
 * Schema can be constructed from following JSON string:
 *
 * {
 *   // Names of all the fields and their types.
 *   // Data types supported: int, long, float, double, string
 *   "fieldTypes": {"publisherId":"java.lang.Integer", "advertiserId":"java.lang.Long", "adUnit":"java.lang.Integer", "clicks":"java.lang.Long", "price":"java.lang.Long"},
 *
 *   // Combinations to use for dimensional computations
 *   // If omitted, all combinations of keys are grouped by MINUTE
 *   // Supported time groupings: MINUTE, HOUR, DAY
 *   "dimensions": ["time=MINUTE:publisherId:advertiserId", "time=HOUR:advertiserId:adUnit"],
 *
 *   // Fields to aggregate with matching operation types and data types
 *   // Possible operations include: SUM, AVG, MIN, MAX
 *   // Data types supported: int, long, float, double
 *   "aggregates": { "clicks": "sum", "price": "sum" },
 *
 *   // Name of the timestamp fields with time specified in milliseconds ( since Jan 1, 1970 GMT )
 *   // Data type is implied to be: long
 *   "timeKey": "timestamp",
 * }
 *
 *
 */
public class EventSchema implements Serializable
{
  private static final long serialVersionUID = 4586481500190519858L;

  /* Map of all the fields and their types in an event tuple */
  public Map<String, Class<?>> fieldTypes = Maps.newHashMap();

  /* Names of the fields which make up the keys */
  public List<String> keys = Lists.newArrayList();

  // Fields to aggregate mapped to aggregate operations and data types
  public Map<String, String> aggregates = Maps.newHashMap();

  // List of dimensional combinations to compute
  public List<String> dimensions = Lists.newArrayList();

  private String timeKey = "timestamp";

  transient private int keyLen;
  transient private int valLen;

  public static EventSchema createFromJSON(String json) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    EventSchema eventSchema = mapper.readValue(json, EventSchema.class);
    Set<String> uniqueKeys = Sets.newHashSet();
    for(String dimension: eventSchema.dimensions) {
      String[] attributes = dimension.split(":");
      for (String attribute : attributes) {
        String[] keyval = attribute.split("=", 2);
        String key = keyval[0];
        if (key.equals("time")) {
          continue;
        }
        uniqueKeys.add(key);
      }
    }
    uniqueKeys.add(eventSchema.getTimeKey());
    List<String> keys = Lists.newArrayList();
    keys.addAll(uniqueKeys);
    eventSchema.setKeys(keys);
    return eventSchema;
  }

  public String getTimeKey() {
    return timeKey;
  }

  public void setFieldTypes(Map<String, Class<?>> fieldTypes)
  {
    this.fieldTypes = fieldTypes;
  }

  public void setKeys(List<String> keys)
  {
    this.keys = keys;
  }

  public Collection<String> getAggregateKeys() {
    return aggregates.keySet();
  }

  public void setAggregates(Map<String, String> aggregates)
  {
    this.aggregates = aggregates;
  }

  public Class<?> getClass(String field) {
    return fieldTypes.get(field);
  }

  public int getKeyLen() {
    if (keyLen == 0)
      keyLen = getSerializedLength(keys);
    return keyLen;
  }

  public int getValLen() {
    if (valLen == 0)
      valLen = getSerializedLength(getAggregateKeys());
    return valLen;
  }

  public int getSerializedLength(Collection<String> fields) {
    int len = 0;
    for(String field : fields) {
      Class<?> k = fieldTypes.get(field);
      len += GenericEventSerializer.fieldSerializers.get(k).dataLength();
    }
    return len;
  }

  public Class<?> getType(String param)
  {
    return fieldTypes.get(param);
  }

  public Object typeCast(String input, String fieldKey) {
    Class<?> c = getType(fieldKey);

    if (c.equals(Integer.class)) return Integer.valueOf(input);
    else if (c.equals(Long.class)) return Long.valueOf(input);
    else if (c.equals(Float.class)) return Float.valueOf(input);
    else if (c.equals(Double.class)) return Float.valueOf(input);
    else return input;
  }

  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
  }


}
