package com.datatorrent.demos.adsdimension.generic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import junit.framework.Assert;

import org.junit.Test;

import java.util.List;
import java.util.Map;

public class GenericEventSerializerTest
{
  /**
   * Return a EventDescrition object, to be used by operator to
   * perform aggregation, serialization and deserialization.
   * @return
   */
  public static EventSchema getEventSchema() {
    EventSchema eDesc = new EventSchema();

    Map<String, Class<?>> dataDesc  = Maps.newHashMap();
    dataDesc.put("timestamp", Long.class);
    dataDesc.put("pubId", Integer.class);
    dataDesc.put("adId", Integer.class);
    dataDesc.put("adUnit", Integer.class);

    dataDesc.put("clicks", Long.class);
    eDesc.setFieldTypes(dataDesc);

    String[] keys = { "timestamp", "pubId", "adId", "adUnit" };
    List<String> keyDesc = Lists.newArrayList(keys);
    eDesc.setKeys(keyDesc);

    Map<String, String> aggrDesc = Maps.newHashMap();
    aggrDesc.put("clicks", "sum");
    eDesc.setAggregates(aggrDesc);

    return eDesc;
  }

  @Test
  public void test()
  {
    EventSchema eventSchema = getEventSchema();
    GenericEventSerializer ser = new GenericEventSerializer(eventSchema);

    System.out.println("keySize " + eventSchema.getKeyLen() + " val len " + eventSchema.getValLen());

    /* prepare a object */
    MapAggregate event = new MapAggregate(eventSchema);
    event.fields.put("timestamp", System.currentTimeMillis());
    event.fields.put("pubId", 1);
    event.fields.put("adUnit", 2);
    event.fields.put("adId", 3);
    event.fields.put("clicks", new Long(10));

    /* serialize and deserialize object */
    byte[] keyBytes = ser.getKey(event);
    byte[] valBytes = ser.getValue(event);

    MapAggregate o = ser.fromBytes(keyBytes, valBytes);

    org.junit.Assert.assertNotSame("deserialized", event, o);

    Assert.assertEquals(o, event);
    Assert.assertEquals("pubId", o.get("pubId"), event.get("pubId"));
    Assert.assertEquals("pubId", o.get("adUnit"), event.get("adUnit"));
    Assert.assertEquals("pubId", o.get("adId"), event.get("adId"));
    Assert.assertEquals("pubId", o.get("clicks"), event.get("clicks"));

    Assert.assertEquals("timestamp type ", o.get("timestamp").getClass(), Long.class);
    Assert.assertEquals("pubId type ", o.get("pubId").getClass(), Integer.class);
    Assert.assertEquals("adId type ", o.get("adId").getClass(), Integer.class);
    Assert.assertEquals("adUnit type ", o.get("adUnit").getClass(), Integer.class);
    Assert.assertEquals("click type ", o.get("clicks").getClass(), Long.class);
  }

  /* Test with missing fields, serialized with default values */
  @Test
  public void test1()
  {
    EventSchema eventSchema = getEventSchema();
    GenericEventSerializer ser = new GenericEventSerializer(eventSchema);

    System.out.println("keySize " + eventSchema.getKeyLen() + " val len " + eventSchema.getValLen());

    /* prepare a object */
    MapAggregate event = new MapAggregate(eventSchema);
    event.fields.put("timestamp", System.currentTimeMillis());
    event.fields.put("pubId", 1);
    event.fields.put("adUnit", 2);
    event.fields.put("clicks", new Long(10));

    /* serialize and deserialize object */
    byte[] keyBytes = ser.getKey(event);
    byte[] valBytes = ser.getValue(event);

    MapAggregate o = ser.fromBytes(keyBytes, valBytes);

    //Assert.assertEquals(o, event);
    Assert.assertEquals("pubId", o.get("pubId"), event.get("pubId"));
    Assert.assertEquals("pubId", o.get("adUnit"), event.get("adUnit"));
    Assert.assertEquals("pubId", event.get("adId"), null);
    Assert.assertEquals("pubId", o.get("adId"), 0);
    Assert.assertEquals("pubId", o.get("clicks"), event.get("clicks"));

    Assert.assertEquals("timestamp type ", o.get("timestamp").getClass(), Long.class);
    Assert.assertEquals("pubId type ", o.get("pubId").getClass(), Integer.class);
    Assert.assertEquals("adId type ", o.get("adId").getClass(), Integer.class);
    Assert.assertEquals("adUnit type ", o.get("adUnit").getClass(), Integer.class);
    Assert.assertEquals("click type ", o.get("clicks").getClass(), Long.class);
  }

}
