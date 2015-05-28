package com.datatorrent.contrib.store;

import com.datatorrent.common.util.Slice;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Index
{
  private final int MAX_LEVELS = 9;
  private LevelIndex[] levels = new LevelIndex[MAX_LEVELS];

  public LevelIndex get(int i) { return levels[i]; }

  void save(DataOutputStream bos) throws IOException
  {
    for(LevelIndex lidx : levels) {
      lidx.save(bos);
    }
    bos.flush();
  }

  void load(DataInputStream dis) throws IOException
  {
    for(LevelIndex lidx : levels) {
      lidx.load(dis);
    }
  }
}

class LevelIndex {
  List<FileInfo> files = new ArrayList<FileInfo>();
  Map<String, FileInfo> idx = new HashMap<String, FileInfo>();

  FileInfo get(String file) {
    return idx.get(file);
  }

  void addFile(FileInfo info) {
    files.add(info);
    idx.put(info.path, info);
  }

  public void save(DataOutputStream bos) throws IOException
  {
    bos.writeInt(files.size());
    for(FileInfo finfo : files) {
      finfo.save(bos);
    }
  }

  public void load(DataInputStream dis) throws IOException
  {
    int totalFiles = dis.readInt();
    for(int i = 0; i < totalFiles; i++) {
      FileInfo finfo = new FileInfo();
      finfo.load(dis);
    }
  }

  public int numFiles()
  {
    return files != null? files.size() : 0;
  }

  public FileInfo get(int i) {
    return files.get(i);
  }
}

class FileInfo {
  public String path;
  public long size;
  public long totalKeys;
  public Slice startKey;
  public Slice lastKey;

  public FileInfo() { }

  public FileInfo(String fileName, int size, int count, Slice startKey, Slice endKey)
  {
    this.path = fileName;
    this.size = size;
    this.totalKeys = count;
    this.startKey = startKey;
    this.lastKey = endKey;
  }

  public void save(DataOutputStream bos)
  {
    Output out = new Output(bos);
    out.writeString(path);
    out.writeLong(size);
    out.writeLong(totalKeys);
    out.writeInt(startKey.length);
    out.writeBytes(startKey.buffer);
    out.writeInt(lastKey.length);
    out.writeBytes(lastKey.buffer);
    out.flush();
  }

  public void load(DataInputStream dis)
  {
    Input in = new Input(dis);
    path = in.readString();
    size = in.readLong();
    totalKeys = in.readLong();

    int len = in.readInt();
    byte[] bytes = new byte[len];
    in.readBytes(bytes);
    startKey = new Slice(bytes);

    len = in.readInt();
    byte[] bytes1 = new byte[len];
    in.readBytes(bytes1);
    lastKey = new Slice(bytes1);
  }
}