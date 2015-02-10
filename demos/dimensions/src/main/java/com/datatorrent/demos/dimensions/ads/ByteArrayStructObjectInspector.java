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
package com.datatorrent.demos.dimensions.ads;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

public class ByteArrayStructObjectInspector extends StandardStructObjectInspector
{
  ByteArrayStructOIOptions options = null;
  public ByteArrayStructObjectInspector(List<String> structFieldNames,
          List<ObjectInspector> structFieldObjectInspectors,ByteArrayStructOIOptions opts)
  {
    super(structFieldNames, structFieldObjectInspectors);
    options=opts;
  }

  /**
   * Extract the data from the requested field.
   *
   * @param data
   * @param fieldRef
   * @return
   */
  @Override
  public Object getStructFieldData(Object data, StructField fieldRef)
  {
    if (data == null) {
      return null;
    }

    //Byte[] bobj = (Byte[])data;
    AdInfo.AdInfoAggregateEvent adInfo = (AdInfo.AdInfoAggregateEvent)data;

    MyField f = (MyField)fieldRef;

    int fieldID = f.getFieldID();
    assert (fieldID >= 0 && fieldID < fields.size());
    String fieldName = f.getFieldName();
    try {
      if (fieldName.equals("AdUnit")) {
        return adInfo.getAdUnit();
      }
      if (fieldName.equals("PublisherId")) {
        return adInfo.getPublisherId();
      }
      if (fieldName.equals("advertiserId")) {
        return adInfo.getAdvertiserId();
      }
      if (fieldName.equals("cost")) {
        return adInfo.getCost();
      }
      if (fieldName.equals("clicks")) {
        return adInfo.getClicks();
      }
      if (fieldName.equals("impressions")) {
        return adInfo.getImpressions();
      }
      if (fieldName.equals("revenue")) {
        return adInfo.getRevenue();
      }
      if (fieldName.equals("timestamp")) {
        return adInfo.getTimestamp();
      }
    }
    catch (Exception ex) {
      // if key does not exist
      return null;
    }
    return null;
  }

  static List<Object> values = new ArrayList<Object>();

  @Override
  public List<Object> getStructFieldsDataAsList(Object o)
  {
    AdInfo.AdInfoAggregateEvent adInfo = (AdInfo.AdInfoAggregateEvent)o;
    values.clear();

    for (int i = 0; i < fields.size(); i++) {
      String fieldName = fields.get(i).getFieldName();
      try {
        if (fieldName.equals("AdUnit")) {
          values.add(adInfo.getAdUnit());
        }
        if (fieldName.equals("PublisherId")) {
          values.add(adInfo.getPublisherId());
        }
        if (fieldName.equals("advertiserId")) {
          values.add(adInfo.getAdvertiserId());
        }
        if (fieldName.equals("timestamp")) {
          values.add(adInfo.getTimestamp());
        }
        if (fieldName.equals("revenue")) {
          values.add(adInfo.getRevenue());
        }
        if (fieldName.equals("impressions")) {
          values.add(adInfo.getImpressions());
        }
        if (fieldName.equals("clicks")) {
          values.add(adInfo.getClicks());
        }
        if (fieldName.equals("cost")) {
          values.add(adInfo.getCost());
        }
      }
      catch (Exception ex) {
        // we're iterating through the keys so
        // this should never happen
        throw new RuntimeException("Key not found" + ex.getMessage());
      }
    }

    return values;
  }

  /**
   * called to map from hive to adinfo
   *
   * @param fr
   * @return
   */
   protected String getByteArrayField(StructField fr)
   {
   if (options.getMappings() != null && options.getMappings().containsKey(fr.getFieldName())) {
   return options.getMappings().get(fr.getFieldName());
   }
   else {
   return fr.getFieldName();
   }
   }
}
