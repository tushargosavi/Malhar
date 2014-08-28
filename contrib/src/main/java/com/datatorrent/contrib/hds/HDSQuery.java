package com.datatorrent.contrib.hds;

import org.apache.hadoop.hbase.util.Base64;

import java.util.Arrays;

public class HDSQuery
{
  static class QueryParameters
  {
    public String id;
    public long bucketKey;
    public String key;

    @Override public String toString()
    {
      return "QueryParameters{" +
          "id='" + id + '\'' +
          ", bucketKey=" + bucketKey +
          ", key='" + key + '\'' +
          '}';
    }
  }

  static class QueryResult
  {
    public String id;
    public long bucketKey;
    public String key;
    public String value;

    @Override public String toString()
    {
      return "QueryResult{" +
          "id='" + id + '\'' +
          ", bucketKey=" + bucketKey +
          ", key='" + key + '\'' +
          ", value='" + value + '\'' +
          '}';
    }
  }

  @Override public String toString()
  {
    return "HDSQuery{" +
        "id='" + id + '\'' +
        ", bucketKey=" + bucketKey +
        ", key=" + Base64.encodeBytes(key) +
        '}';
  }

  public String id;
  public long bucketKey;
  public byte[] key;
}
