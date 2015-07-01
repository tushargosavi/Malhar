/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.query.serde;

import com.datatorrent.lib.appdata.query.serde.MessageType;
import com.datatorrent.lib.appdata.schemas.Query;
import com.datatorrent.lib.appdata.query.serde.MessageDeserializerInfo;
import com.datatorrent.lib.appdata.query.serde.MessageValidatorInfo;
import com.datatorrent.lib.appdata.query.serde.SimpleDataDeserializer;
import com.datatorrent.lib.appdata.query.serde.SimpleDataValidator;
import org.junit.Assert;
import org.junit.Test;

public class SimpleDataValidatorTest
{
  @Test
  public void testValidatingQuery()
  {
    TestQuery testQuery = new TestQuery("1");
    SimpleDataValidator sqv = new SimpleDataValidator();

    Assert.assertTrue("The query object is not valid.", sqv.validate(testQuery, null));
  }

  @MessageType(type = TestQuery.TYPE)
  @MessageDeserializerInfo(clazz = SimpleDataDeserializer.class)
  @MessageValidatorInfo(clazz = SimpleDataValidator.class)
  public static class TestQuery extends Query
  {
    public static final String TYPE = "testQuery";

    public TestQuery(String id)
    {
      super(id);
    }
  }
}
