package com.datatorrent.contrib.enrichment;

import com.esotericsoftware.kryo.NotNull;
import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FsBackupStore extends ReadOnlyEnrichmentBackup
{
  @NotNull
  private String fileName;

  private transient Path filePath;
  private transient FileSystem fs;
  private transient boolean connected;

  private transient static final ObjectMapper mapper = new ObjectMapper();
  private transient static final ObjectReader reader = mapper.reader(new TypeReference<Map<String, Object>>()
  {
  });
  private transient static final Logger logger = LoggerFactory.getLogger(FsBackupStore.class);

  public String getFileName()
  {
    return fileName;
  }

  public void setFileName(String fileName)
  {
    this.fileName = fileName;
  }

  @Override public Map<Object, Object> loadInitialData()
  {
    Map<Object, Object> result = null;
    try {
      result = Maps.newHashMap();
      FSDataInputStream in = fs.open(filePath);
      BufferedReader bin = new BufferedReader(new InputStreamReader(in));
      String line;
      while ((line = bin.readLine()) != null) {
        try {
          Map<String, Object> tuple = reader.readValue(line);
          ArrayList<Object> includeTuple = new ArrayList<Object>();
          for(String s: includeFields) {
            includeTuple.add(tuple.get(s));
          }
          result.put(getKey(tuple), includeTuple);
        } catch (JsonProcessingException parseExp) {
          logger.info("Unable to parse line {}", line);
        }
      }
      IOUtils.closeQuietly(bin);
      IOUtils.closeQuietly(in);
    } catch (IOException e) {
      e.printStackTrace();
    }
    logger.debug("loading initial data {}", result.size());
    logger.debug("{}", result);
    return result;
  }

  private Object getKey(Map<String, Object> tuple)
  {
    ArrayList<Object> lst = new ArrayList<Object>();
    for(String key : lookupFields) {
      lst.add(tuple.get(key));
    }
    return lst;
  }

  @Override public Object get(Object key)
  {
    return null;
  }

  @Override public List<Object> getAll(List<Object> keys)
  {
    return null;
  }

  @Override public void connect() throws IOException
  {
    Configuration conf = new Configuration();
    this.filePath = new Path(fileName);
    this.fs = FileSystem.newInstance(filePath.toUri(), conf);
    if (!fs.isFile(filePath))
      throw new IOException("Provided path " + fileName + " is not a file");
    connected = true;
  }

  @Override public void disconnect() throws IOException
  {
    if (fs != null)
      fs.close();
  }

  @Override public boolean isConnected()
  {
    return connected;
  }

  @Override
  public boolean needRefresh() {
    return false;
  }
}
