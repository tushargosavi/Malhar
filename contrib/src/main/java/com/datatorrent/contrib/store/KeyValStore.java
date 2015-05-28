package com.datatorrent.contrib.store;

import com.datatorrent.common.util.Slice;

public interface KeyValStore
{
  void put(Slice key, Slice value);
  Slice get(Slice key);

  /*
    void commit();

  interface Scanner {
    void rewind();
    void seek(Slice key);
    void upperBound(Slice key);
    void lowerBound(Slice key);
  }
  */
}
