package com.datatorrent.lib.hds;

import com.datatorrent.lib.util.KeyValPair;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

public class BaseKeyVal extends KeyValPair<byte[], byte[]> implements Comparable<BaseKeyVal>, HDS.BucketKey
{

  public BaseKeyVal(byte[] key, byte[] val) {
    super(key, val);
  }

  public static int byteCompare(byte[] key1, byte[] key2)
  {
    int i, j, diff;

    i = j = diff = 0;

    while (i < key1.length && j < key2.length) {
      diff = key1[i++] - key2[j++];
      if (diff != 0)
        return diff;
    }
    if (i == key1.length && j == key2.length)
      return diff;
    if (i == key1.length)
      return -1;
    return 1;
  }

  /* First compare values, if key matches then compare data */
  public static int compare(BaseKeyVal o1, BaseKeyVal o2)
  {
    byte[] key1 = o1.getKey();
    byte[] key2 = o2.getKey();

    int diff = byteCompare(key1, key2);
    if (diff != 0)
      return diff;

    return byteCompare(o1.getValue(), o2.getValue());
  }

  @Override public int compareTo(BaseKeyVal o)
  {
    return compare(this, o);
  }



  private static class BaseKeyValSerializer implements HDS.WalSerializer<BaseKeyVal> {

    @Override public byte[] toBytes(BaseKeyVal data)
    {
      byte[] key = data.getKey();
      byte[] val = data.getValue();
      int keyLen = key.length;
      int valLen = val.length;

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(keyLen);
      out.write(key, 0, keyLen);
      out.write(valLen);
      out.write(val, 0, valLen);
      return out.toByteArray();
    }

    @Override public BaseKeyVal fromBytes(byte[] arr)
    {
      ByteArrayInputStream bin = new ByteArrayInputStream(arr);
      int keyLen = bin.read();
      byte[] key = new byte[keyLen];
      bin.read(key, 0, keyLen);
      int valLen = bin.read();
      byte[] val = new byte[valLen];
      bin.read(val, 0, valLen);
      return new BaseKeyVal(key, val);
    }
  }

  public static final Comparator<BaseKeyVal> DEFAULT_COMPARATOR = new Comparator<BaseKeyVal>()
  {
    @Override public int compare(BaseKeyVal o1, BaseKeyVal o2)
    {
      return compare(o1, o2);
    }
  };

  public static final HDS.WalSerializer<BaseKeyVal> DEFAULT_SERIALIZER = new BaseKeyValSerializer();

  @Override public long getBucketKey()
  {
    return ByteBuffer.wrap(getKey()).getInt();
  }
}
