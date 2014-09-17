package com.datatorrent.demos.adsdimension;

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
  public static EventDescription getDataDesc() {
    EventDescription eDesc = new EventDescription();

    Map<String, Class> dataDesc  = Maps.newHashMap();
    dataDesc.put("timestamp", Long.class);
    dataDesc.put("pubId", Integer.class);
    dataDesc.put("adId", Integer.class);
    dataDesc.put("adUnit", Integer.class);

    dataDesc.put("clicks", Long.class);
    eDesc.setDataDesc(dataDesc);

    String[] keys = { "timestamp", "pubId", "adId", "adUnit" };
    List<String> keyDesc = Lists.newArrayList(keys);
    eDesc.setKeys(keyDesc);

    String[] vals = { "clicks" };
    List<String> valDesc = Lists.newArrayList(vals);
    eDesc.setMetrices(valDesc);

    Map<String, String> aggrDesc = Maps.newHashMapWithExpectedSize(vals.length);
    aggrDesc.put("clicks", "sum");
    eDesc.setAggrDesc(aggrDesc);

    String[] partitionDesc = { "pubId" };
    List<String> partDesc = Lists.newArrayList(partitionDesc);
    eDesc.setPartitionKeys(partDesc);

    return eDesc;
  }

  @Test
  public void test()
  {
    EventDescription eDesc = getDataDesc();
    GenericEventSerializer ser = new GenericEventSerializer(eDesc);

    System.out.println("keySize " + eDesc.getKeyLen() + " val len " + eDesc.getValLen());

    /* prepare a object */
    MapAggregateEvent event = new MapAggregateEvent(0);
    event.keys.put("timestamp", System.currentTimeMillis());
    event.keys.put("pubId", 1);
    event.keys.put("adUnit", 2);
    event.keys.put("adId", 3);
    event.fields.put("clicks", new Long(10));

    /* serialize and deserialize object */
    byte[] keyBytes = ser.getKey(event);
    byte[] valBytes = ser.getValue(event);

    MapAggregateEvent o = ser.fromBytes(keyBytes, valBytes);

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
    EventDescription eDesc = getDataDesc();
    GenericEventSerializer ser = new GenericEventSerializer(eDesc);

    System.out.println("keySize " + eDesc.getKeyLen() + " val len " + eDesc.getValLen());

    /* prepare a object */
    MapAggregateEvent event = new MapAggregateEvent(0);
    event.keys.put("timestamp", System.currentTimeMillis());
    event.keys.put("pubId", 1);
    event.keys.put("adUnit", 2);
    event.fields.put("clicks", new Long(10));

    /* serialize and deserialize object */
    byte[] keyBytes = ser.getKey(event);
    byte[] valBytes = ser.getValue(event);

    MapAggregateEvent o = ser.fromBytes(keyBytes, valBytes);

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
