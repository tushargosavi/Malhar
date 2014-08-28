package com.datatorrent.contrib.hds;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Base64;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Readonly Client for HDS data store, which accepts queries in
 * JSON formmat.
 *
 * Query format.
 * {
 *   'id': id of the
 *   'bucketKey': bucket key
 *   'key' : Base64 encoded key.
 * }
 *
 * Result format.
 * {
 *   'id' : id of query.
 *   'bucketKey': bucket key
 *   'key' : Base64 encoded key.
 *   'value' : Base64 encoded value.
 * }
 *
 */
public class HDSJSonClient
{
  private static final Logger LOG = LoggerFactory.getLogger(HDSJSonClient.class);
  private transient ObjectMapper mapper;
  private List<HDSQuery> queries = Lists.newArrayList();

  HDSBucketManager hds;

  public HDSQuery registerQuery(String queryString) throws Exception
  {
    if (mapper == null) {
      mapper = new ObjectMapper();
      mapper.configure(
          org.codehaus.jackson.map.DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    HDSQuery.QueryParameters queryParams =
        mapper.readValue(queryString, HDSQuery.QueryParameters.class);

    System.out.println("query params " + queryParams);
    if (queryParams.id == null) {
      LOG.error("Missing id  1 in {}", queryString);
      return null;
    }

    if (queryParams.bucketKey == 0) {
      LOG.error("Missing bucketKey 2 in {}", queryString);
      return null;
    }

    if (queryParams.key == null) {
      LOG.error("Missing key selector 3 in {}", queryString);
      return null;
    }

    HDSQuery query = new HDSQuery();
    query.id = queryParams.id;
    query.bucketKey = queryParams.bucketKey;
    query.key = Base64.decode(queryParams.key);

    queries.add(query);
    return query;
  }


  public void setStore(HDSBucketManager hds) {
    this.hds = hds;
  }

  public HDSQuery.QueryResult processQuery(HDSQuery query) throws IOException
  {
    byte[] value = hds.get(query.bucketKey, query.key);
    if (value == null)
      return null;

    HDSQuery.QueryResult result = new HDSQuery.QueryResult();
    result.id = query.id;
    result.bucketKey = query.bucketKey;
    result.key = Base64.encodeBytes(query.key);
    result.value = Base64.encodeBytes(value);
    return result;
  }
}
