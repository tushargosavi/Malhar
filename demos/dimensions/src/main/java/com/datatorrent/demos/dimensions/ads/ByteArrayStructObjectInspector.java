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
  public ByteArrayStructObjectInspector(List<String> structFieldNames,
          List<ObjectInspector> structFieldObjectInspectors)
  {
    super(structFieldNames, structFieldObjectInspectors);
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

    Byte[] bobj = (Byte[])data;
    MyField f = (MyField)fieldRef;

    int fieldID = f.getFieldID();
    assert (fieldID >= 0 && fieldID < fields.size());

    try {
      return bobj[fieldID];
    }
    catch (Exception ex) {
      // if key does not exist
      return null;
    }
  }

  static List<Object> values = new ArrayList<Object>();

  @Override
  public List<Object> getStructFieldsDataAsList(Object o)
  {
    Byte[] bObj = (Byte[])o;
    values.clear();

    for (int i = 0; i < fields.size(); i++) {
      try {
        values.add(bObj[i]);
      }
      catch (Exception ex) {
                // we're iterating through the keys so
        // this should never happen
        throw new RuntimeException("Key not found");
      }
    }

    return values;
  }

}


