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

import junit.framework.Assert;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EventSchemaTest {
  private static final Logger LOG = LoggerFactory.getLogger(JsonAdInfoGeneratorTest.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testCreateFromJSON() throws Exception {

    String sampleJSON = "{\n" +
            "  \"fieldTypes\": {\"publisherId\":\"java.lang.Integer\", \"advertiserId\":\"java.lang.Long\", \"adUnit\":\"java.lang.Integer\", \"clicks\":\"java.lang.Long\", \"price\":\"java.lang.Long\"},\n" +
            "  \"dimensions\": [\"time=MINUTE:publisherId:advertiserId\", \"time=HOUR:advertiserId:adUnit\"],\n" +
            "  \"aggregates\": { \"clicks\": \"sum\", \"price\": \"sum\" },\n" +
            "  \"timeKey\": \"timestamp\"\n" +
            "}\n";
    EventSchema eventSchema = EventSchema.createFromJSON(sampleJSON);

    LOG.debug("Evaluating EventSchema: {}", eventSchema.toString());

    Assert.assertNotNull("EventSchema was created", eventSchema);
    Assert.assertEquals("timeKey is timestamp", eventSchema.getTimeKey(), "timestamp");
    Assert.assertTrue("aggregates are have clicks", eventSchema.getAggregateKeys().contains("clicks") );
    Assert.assertTrue("aggregate for price is sum", eventSchema.aggregates.get("price").equals("sum") );
    Assert.assertTrue("fieldType publisher is Integer", eventSchema.fieldTypes.get("publisherId").equals(Integer.class));
    Assert.assertTrue("fieldType clicks is Long", eventSchema.fieldTypes.get("clicks").equals(Long.class));
    Assert.assertTrue("dimensions include time=MINUTE:publisherId:advertiserId", eventSchema.dimensions.contains("time=MINUTE:publisherId:advertiserId"));
  }


}
