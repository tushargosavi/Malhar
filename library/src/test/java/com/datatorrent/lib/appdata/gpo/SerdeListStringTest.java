/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.lib.appdata.gpo;

import com.datatorrent.lib.appdata.gpo.SerdeListString;
import com.datatorrent.lib.appdata.gpo.GPOByteArrayList;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SerdeListStringTest
{
  @Test
  public void simpleSerdeTest()
  {
    SerdeListString sls = SerdeListString.INSTANCE;

    List<String> testList = Lists.newArrayList("timothy", "farkas", "is", "the", "coolest");
    byte[] serializedObject = sls.serializeObject(testList);

    GPOByteArrayList gpoBytes = new GPOByteArrayList();
    byte[] bytesA = new byte[20];
    byte[] bytesB = new byte[13];

    gpoBytes.add(bytesA);
    gpoBytes.add(serializedObject);
    gpoBytes.add(bytesB);

    MutableInt intVals = new MutableInt(bytesA.length);

    @SuppressWarnings("unchecked")
    List<String> deserializedList =
    (List<String>) sls.deserializeObject(gpoBytes.toByteArray(), intVals);

    Assert.assertEquals(testList, deserializedList);
  }
}
