/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.demos.adsdimension.generic;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.demos.adsdimension.AdInfo;
import com.datatorrent.demos.adsdimension.InputItemGenerator;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Generates AdInfo events and sends them out in JSON format.
 *
 * By default 10000 events are generated in every window.  See {@link com.datatorrent.demos.adsdimension.InputItemGenerator}
 * for additional details on tunable settings settings like blastCount, numPublishers, numAdvertisers, numAdUnits, etc.
 *
 */
public class JsonSalesInfoGenerator extends SalesDataGenerator
{
  @OutputPortFieldAnnotation(name = "jsonOutput")
  public final transient DefaultOutputPort<byte[]> jsonOutput = new DefaultOutputPort<byte[]>();
  private static final ObjectMapper mapper = new ObjectMapper();

  @Override
  public void emitTuple(SaleInfo saleInfo)
  {
    try {
      this.jsonOutput.emit(mapper.writeValueAsBytes(saleInfo));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

