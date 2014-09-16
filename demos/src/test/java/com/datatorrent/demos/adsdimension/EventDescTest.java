package com.datatorrent.demos.adsdimension;

import org.junit.Test;

import java.io.IOException;


public class EventDescTest
{
  @Test
  public void test1() throws IOException
  {
    String desc = "{" +
      "\"fields\": [ {\"publisherId\": \"int\"}, {\"advertiserId\": \"int\"}, {\"adUnit\" : \"int\"}, {\"clicks\":\"long\"}]," +
      "\"keys\": [\"publisherId\", \"advertiserId\", \"adUnit\"]," +
      "\"metrices\": [ \"clicks\"]," +
      "\"aggrDesc\" : [ \"clicks\":\"sum\"]," +
      "\"partitionKeys\" : [\"publisherId\"]" +
    "}";

    EventDescription e = EventDescription.create(desc);
    System.out.println(e);
  }
}

