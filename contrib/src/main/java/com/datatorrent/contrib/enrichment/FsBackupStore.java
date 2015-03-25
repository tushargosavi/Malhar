package com.datatorrent.contrib.enrichment;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FsBackupStore extends ReadOnlyBackup
{
  @Override public Map<Object, Object> loadInitialData()
  {
    return null;
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

  }

  @Override public void disconnect() throws IOException
  {

  }

  @Override public boolean isConnected()
  {
    return true;
  }
}
